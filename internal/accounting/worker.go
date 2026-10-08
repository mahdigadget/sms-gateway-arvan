package accounting

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sms-gateway-arvan/internal/cache"
	kafkaPlatform "sms-gateway-arvan/internal/platform/kafka"
	"sms-gateway-arvan/internal/platform/telemetry"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var (
	ErrInsufficientBalance = errors.New("insufficient balance or wallet not found")
	ErrReleaseReserve      = errors.New("reserve must be released")
)

type Worker struct {
	brokers       string
	groupID       string
	db            *sql.DB
	rdb           *redis.Client
	balanceClient *cache.BalanceClient
	consumer      *kafka.Consumer

	batchSize     int
	flushInterval time.Duration

	// dbDown را health می‌نویسد. Pause خود کانسیومر فقط روی گوروتین Poll است.
	dbDown           bool
	partitionsPaused bool
	mu               sync.Mutex
}

type KafkaMessagePayload struct {
	TenantID    string    `json:"tenant_id"`
	Destination string    `json:"destination"`
	Sender      string    `json:"sender"`
	Body        string    `json:"body"`
	DeadlineAt  int64     `json:"deadline_at,omitempty"` // ثانیه؛ نرمال این فیلد را ندارد
	CreatedAt   time.Time `json:"created_at"`
	IdempHash   string    `json:"idemp_hash"`
}

type ConsumedRecord struct {
	ID             uuid.UUID
	TenantID       string // همان رشتهٔ درخواست؛ کلید ردیس با همین ساخته شده
	CustomerID     uuid.UUID
	IdempotencyKey string
	RequestHash    []byte
	Kind           string
	Recipient      string
	Sender         string
	Body           string
	Cost           int64
	DeadlineAt     *time.Time
	CreatedAt      time.Time
	KafkaMsg       *kafka.Message
}

func NewWorker(brokers, groupID string, db *sql.DB, rdb *redis.Client) *Worker {
	return &Worker{
		brokers:       brokers,
		groupID:       groupID,
		db:            db,
		rdb:           rdb,
		balanceClient: cache.NewBalanceClient(rdb),
		batchSize:     100,
		flushInterval: 300 * time.Millisecond,
	}
}

func (w *Worker) Start(ctx context.Context) {
	config := &kafka.ConfigMap{
		"bootstrap.servers":    w.brokers,
		"group.id":             w.groupID,
		"auto.offset.reset":    "earliest",
		"enable.auto.commit":   false,
		"max.poll.interval.ms": 300000,
	}
	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		log.Fatalf("failed to create accounting kafka consumer: %v", err)
	}
	w.consumer = consumer
	defer func() { _ = w.consumer.Close() }()

	topics := []string{kafkaPlatform.TopicVIP, kafkaPlatform.TopicNormal}
	if err = w.consumer.SubscribeTopics(topics, nil); err != nil {
		log.Fatalf("failed to subscribe accounting topics: %v", err)
	}

	log.Printf("accounting worker started (groups: %s, batch: %d)", w.groupID, w.batchSize)

	go w.startDBHealthChecker(ctx)

	batch := make([]*ConsumedRecord, 0, w.batchSize)
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("stopping accounting worker, flushing remaining messages...")
			if len(batch) > 0 {
				_ = w.flush(context.Background(), batch)
			}
			return
		case <-ticker.C:
			if len(batch) > 0 {
				if err = w.flush(ctx, batch); err == nil {
					batch = batch[:0]
				}
			}
		default:
			w.applyDBPause()
			// بچ پر است و flush خطا داده. Poll را قطع نمی‌کنیم تا از گروه بیرون نیفتیم.
			if len(batch) >= w.batchSize {
				if err = w.flush(ctx, batch); err != nil {
					ev := w.consumer.Poll(50)
					batch = w.absorb(ctx, ev, batch)
					continue
				}
				batch = batch[:0]
			}
			ev := w.consumer.Poll(50)
			if ev == nil {
				continue
			}
			batch = w.absorb(ctx, ev, batch)
		}
	}
}

// absorb پیام را به بچ اضافه می‌کند. پیام خراب آفستش می‌رود تا پارتیشن قفل نشود.
func (w *Worker) absorb(ctx context.Context, ev kafka.Event, batch []*ConsumedRecord) []*ConsumedRecord {
	if ev == nil {
		return batch
	}
	switch e := ev.(type) {
	case *kafka.Message:
		record, err := w.parseMessage(e)
		if err != nil {
			log.Printf("corrupted message in accounting, dropping: %v", err)
			w.dropPoison(ctx, e)
			_, _ = w.consumer.CommitMessage(e)
			return batch
		}
		return append(batch, record)
	case kafka.Error:
		log.Printf("accounting consumer error: %v", e)
	default:
	}
	return batch
}

// dropPoison اگر tenant و هش را داریم رزرو را ول می‌کنیم. اسکریپت بار دوم no-op است.
func (w *Worker) dropPoison(ctx context.Context, msg *kafka.Message) {
	var payload KafkaMessagePayload
	if json.Unmarshal(msg.Value, &payload) != nil {
		return
	}
	if payload.TenantID == "" || payload.IdempHash == "" {
		return
	}
	if err := w.balanceClient.Rollback(ctx, payload.TenantID, payload.IdempHash); err != nil {
		log.Printf("[ACCOUNTING-ERROR] rollback poison message: %v", err)
	}
}

// flush: استراتژی Bulk با فال‌بک به پردازش تکی جهت جلوگیری از Head-of-Line Blocking
func (w *Worker) flush(ctx context.Context, batch []*ConsumedRecord) (err error) {
	if len(batch) == 0 {
		return nil
	}
	ctx, endTrace := telemetry.ObserveBatch(ctx, kafkaRefs(batch))
	defer func() { endTrace(err) }()

	// تلاش اولیه در قالب یک ترنزکشن بالک سریع (Happy Path)
	err = w.flushBulk(ctx, batch)
	if err == nil {
		return w.commitBatch(ctx, batch)
	}

	// اگر خطا ناشی از قطعی دیتابیس یا کانتکست باشد، فال‌بک فایده‌ای ندارد و باید کل بچ ریترای شود
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	telemetry.Printf(ctx, "[ACCOUNTING-WARN] bulk flush failed (%v), falling back to individual processing...", err)

	// فعال‌سازی حالت Fallback: پردازش دانه‌به‌دانه رکوردها
	if err = w.fallbackProcessIndividually(ctx, batch); err != nil {
		return err
	}

	return w.commitBatch(ctx, batch)
}

// commitBatch آخرین آفست هر پارتیشن داخل بچ را می‌نویسد.
func (w *Worker) commitBatch(ctx context.Context, batch []*ConsumedRecord) error {
	last := make(map[string]*kafka.Message)
	for _, item := range batch {
		msg := item.KafkaMsg
		if msg == nil || msg.TopicPartition.Topic == nil {
			continue
		}
		key := fmt.Sprintf("%s:%d", *msg.TopicPartition.Topic, msg.TopicPartition.Partition)
		prev := last[key]
		if prev == nil || msg.TopicPartition.Offset > prev.TopicPartition.Offset {
			last[key] = msg
		}
	}
	for _, msg := range last {
		if _, err := w.consumer.CommitMessage(msg); err != nil {
			telemetry.Printf(ctx, "[ACCOUNTING-ERROR] failed to commit kafka offset: %v", err)
			return err
		}
	}
	return nil
}

// flushBulk: اجرای یکجای بچ با یک ترنزکشن
func (w *Worker) flushBulk(ctx context.Context, batch []*ConsumedRecord) error {
	txCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()

	tx, err := w.db.BeginTx(txCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	insertedKeys, err := w.bulkInsertOutbox(txCtx, tx, batch)
	if err != nil {
		return fmt.Errorf("bulk insert outbox failed: %w", err)
	}

	costPerCustomer := make(map[uuid.UUID]int64)
	for _, item := range batch {
		if insertedKeys[item.IdempotencyKey] {
			costPerCustomer[item.CustomerID] += item.Cost
		}
	}

	// ترتیب ثابت قفل ردیف، که آپدیت همزمان با compensation گره نخورد
	customerIDs := make([]uuid.UUID, 0, len(costPerCustomer))
	for customerID, totalCost := range costPerCustomer {
		if totalCost > 0 {
			customerIDs = append(customerIDs, customerID)
		}
	}
	sort.Slice(customerIDs, func(i, j int) bool {
		return customerIDs[i].String() < customerIDs[j].String()
	})

	for _, customerID := range customerIDs {
		totalCost := costPerCustomer[customerID]
		res, err := tx.ExecContext(txCtx, `
			UPDATE wallet
			SET available_balance = available_balance - $1,
			    updated_at = NOW()
			WHERE customer_id = $2 AND available_balance >= $1
		`, totalCost, customerID)
		if err != nil {
			return fmt.Errorf("wallet deduction exec error: %w", err)
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil || rowsAffected == 0 {
			return fmt.Errorf("%w for customer %s", ErrInsufficientBalance, customerID)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}

	// ردیس بعد از کامیت پستگرس. خطا یعنی آفست کافکا نرود تا دوباره همین‌جا تلاش شود.
	return w.commitReserved(ctx, batch)
}

// fallbackProcessIndividually: ایزوله کردن پیام‌های معیوب و کامیت مجزای پیام‌های سالم.
// خطای دیتابیس کل بچ را برمی‌گرداند تا آفست نرود. کمبود موجودی فقط همان پیام را رد می‌کند.
func (w *Worker) fallbackProcessIndividually(ctx context.Context, batch []*ConsumedRecord) error {
	for _, record := range batch {
		err := w.processSingleRecord(ctx, record)
		if err == nil {
			if err = w.balanceClient.Commit(ctx, record.TenantID, record.IdempotencyKey); err != nil {
				return err
			}
			continue
		}
		if errors.Is(err, ErrInsufficientBalance) {
			telemetry.Printf(ctx, "[ACCOUNTING-FALLBACK-FAILED] item %s customer %s rejected: %v", record.IdempotencyKey, record.TenantID, err)
			if recErr := w.recordFailedMessage(ctx, record, err.Error()); recErr != nil {
				return recErr
			}
			if rbErr := w.balanceClient.Rollback(ctx, record.TenantID, record.IdempotencyKey); rbErr != nil {
				return rbErr
			}
			continue
		}
		if errors.Is(err, ErrReleaseReserve) {
			if rbErr := w.balanceClient.Rollback(ctx, record.TenantID, record.IdempotencyKey); rbErr != nil {
				return rbErr
			}
			continue
		}
		return err
	}
	return nil
}

// processSingleRecord: تراکنش مستقل برای تک‌تک پیام‌ها در زمان وقوع خطا
func (w *Worker) processSingleRecord(ctx context.Context, item *ConsumedRecord) error {
	txCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	tx, err := w.db.BeginTx(txCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var existingType string
	err = tx.QueryRowContext(txCtx, `
		SELECT event_type FROM outbox_events WHERE idempotency_key = $1
	`, item.IdempotencyKey).Scan(&existingType)
	if err == nil {
		if existingType == "message.accepted" {
			return tx.Commit()
		}
		// rejected یا refunded. کسر دوباره نمی‌شود، رزرو اگر مانده باشد برمی‌گردد.
		return ErrReleaseReserve
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	payloadBytes, err := json.Marshal(map[string]interface{}{
		"id":              item.ID,
		"customer_id":     item.CustomerID,
		"idempotency_key": item.IdempotencyKey,
		"kind":            item.Kind,
		"recipient":       item.Recipient,
		"sender":          item.Sender,
		"body":            item.Body,
		"cost":            item.Cost,
		"deadline_at":     item.DeadlineAt,
		"created_at":      item.CreatedAt,
	})
	if err != nil {
		return err
	}

	var insertedKey sql.NullString
	err = tx.QueryRowContext(txCtx, `
		INSERT INTO outbox_events (event_type, event_version, payload, idempotency_key)
		VALUES ('message.accepted', 1, $1, $2)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING idempotency_key
	`, payloadBytes, item.IdempotencyKey).Scan(&insertedKey)

	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	// تداخل با ردیف موجود. accepted یعنی فقط رزرو ردیس کم می‌شود؛ بقیه یعنی رزرو برگردد.
	if !insertedKey.Valid {
		var eventType string
		if qerr := tx.QueryRowContext(txCtx, `
			SELECT event_type FROM outbox_events WHERE idempotency_key = $1
		`, item.IdempotencyKey).Scan(&eventType); qerr != nil {
			return qerr
		}
		if eventType != "message.accepted" {
			return ErrReleaseReserve
		}
		return tx.Commit()
	}

	// اگر پیام تکراری نبود، کسر موجودی انجام می‌شود
	if item.Cost > 0 {
		res, err := tx.ExecContext(txCtx, `
			UPDATE wallet
			SET available_balance = available_balance - $1,
			    updated_at = NOW()
			WHERE customer_id = $2 AND available_balance >= $1
		`, item.Cost, item.CustomerID)
		if err != nil {
			return err
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil || rowsAffected == 0 {
			return ErrInsufficientBalance
		}
	}

	return tx.Commit()
}

// recordFailedMessage: ثبت پیام‌های رد شده در outbox جهت پیگیری کلاینت
func (w *Worker) recordFailedMessage(ctx context.Context, item *ConsumedRecord, reason string) error {
	payloadBytes, _ := json.Marshal(map[string]interface{}{
		"id":              item.ID,
		"customer_id":     item.CustomerID,
		"idempotency_key": item.IdempotencyKey,
		"reason":          reason,
		"created_at":      time.Now(),
	})

	_, err := w.db.ExecContext(ctx, `
		INSERT INTO outbox_events (event_type, event_version, payload, idempotency_key)
		VALUES ('message.rejected', 1, $1, $2)
		ON CONFLICT (idempotency_key) DO NOTHING
	`, payloadBytes, item.IdempotencyKey)
	return err
}

func (w *Worker) bulkInsertOutbox(ctx context.Context, tx *sql.Tx, batch []*ConsumedRecord) (map[string]bool, error) {
	if len(batch) == 0 {
		return nil, nil
	}

	valueStrings := make([]string, 0, len(batch))
	valueArgs := make([]interface{}, 0, len(batch)*2)

	for i, item := range batch {
		payloadBytes, err := json.Marshal(map[string]interface{}{
			"id":              item.ID,
			"customer_id":     item.CustomerID,
			"idempotency_key": item.IdempotencyKey,
			"kind":            item.Kind,
			"recipient":       item.Recipient,
			"sender":          item.Sender,
			"body":            item.Body,
			"cost":            item.Cost,
			"deadline_at":     item.DeadlineAt,
			"created_at":      item.CreatedAt,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to marshal payload for %s: %w", item.IdempotencyKey, err)
		}

		baseIdx := i * 2
		valueStrings = append(valueStrings, fmt.Sprintf("('message.accepted', 1, $%d, $%d)", baseIdx+1, baseIdx+2))
		valueArgs = append(valueArgs, payloadBytes, item.IdempotencyKey)
	}

	query := fmt.Sprintf(`
		INSERT INTO outbox_events (event_type, event_version, payload, idempotency_key)
		VALUES %s
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING idempotency_key
	`, strings.Join(valueStrings, ","))

	rows, err := tx.QueryContext(ctx, query, valueArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	inserted := make(map[string]bool)
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		inserted[key] = true
	}

	return inserted, rows.Err()
}

func (w *Worker) startDBHealthChecker(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
			err := w.db.PingContext(pingCtx)
			cancel()

			w.mu.Lock()
			changed := w.dbDown != (err != nil)
			w.dbDown = err != nil
			down := w.dbDown
			w.mu.Unlock()
			if changed && down {
				log.Printf("[ACCOUNTING-HEALTH] DB unreachable (%v)", err)
			}
			if changed && !down {
				log.Printf("[ACCOUNTING-HEALTH] DB reachable again")
			}
		}
	}
}

// applyDBPause فقط از گوروتین Poll صدا زده می‌شود. کانسیومر کافکا thread-safe نیست.
func (w *Worker) applyDBPause() {
	w.mu.Lock()
	down := w.dbDown
	w.mu.Unlock()

	if down && !w.partitionsPaused {
		assignment, err := w.consumer.Assignment()
		if err != nil || len(assignment) == 0 {
			return
		}
		if err = w.consumer.Pause(assignment); err != nil {
			log.Printf("[ACCOUNTING-HEALTH] pause failed: %v", err)
			return
		}
		w.partitionsPaused = true
		log.Printf("[ACCOUNTING-HEALTH] PAUSED consumer partitions")
		return
	}
	if !down && w.partitionsPaused {
		assignment, err := w.consumer.Assignment()
		if err != nil || len(assignment) == 0 {
			return
		}
		if err = w.consumer.Resume(assignment); err != nil {
			log.Printf("[ACCOUNTING-HEALTH] resume failed: %v", err)
			return
		}
		w.partitionsPaused = false
		log.Printf("[ACCOUNTING-HEALTH] RESUMED consumer partitions")
	}
}

// commitReserved برای ردیف‌های message.accepted رزرو ردیس را یک بار کم می‌کند.
func (w *Worker) commitReserved(ctx context.Context, batch []*ConsumedRecord) error {
	if len(batch) == 0 {
		return nil
	}
	holders := make([]string, len(batch))
	args := make([]interface{}, len(batch))
	byKey := make(map[string]*ConsumedRecord, len(batch))
	for i, item := range batch {
		holders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = item.IdempotencyKey
		byKey[item.IdempotencyKey] = item
	}
	query := fmt.Sprintf(`
		SELECT idempotency_key
		FROM outbox_events
		WHERE event_type = 'message.accepted'
		  AND idempotency_key IN (%s)
	`, strings.Join(holders, ","))

	rows, err := w.db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			return err
		}
		item := byKey[key]
		if item == nil {
			continue
		}
		if err = w.balanceClient.Commit(ctx, item.TenantID, item.IdempotencyKey); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (w *Worker) parseMessage(msg *kafka.Message) (*ConsumedRecord, error) {
	var payload KafkaMessagePayload
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		return nil, err
	}

	custUUID, err := uuid.Parse(payload.TenantID)
	if err != nil {
		return nil, fmt.Errorf("invalid tenant UUID: %w", err)
	}

	var deadline *time.Time
	kind := "normal"
	if payload.DeadlineAt > 0 {
		kind = "otp"
		t := payload.CreatedAt.Add(time.Duration(payload.DeadlineAt) * time.Second)
		deadline = &t
	}

	return &ConsumedRecord{
		ID:             uuid.New(),
		TenantID:       payload.TenantID,
		CustomerID:     custUUID,
		IdempotencyKey: payload.IdempHash,
		RequestHash:    []byte(payload.IdempHash),
		Kind:           kind,
		Recipient:      payload.Destination,
		Sender:         payload.Sender,
		Body:           payload.Body,
		Cost:           1,
		DeadlineAt:     deadline,
		CreatedAt:      payload.CreatedAt,
		KafkaMsg:       msg,
	}, nil
}

func kafkaRefs(batch []*ConsumedRecord) []telemetry.KafkaRef {
	refs := make([]telemetry.KafkaRef, 0, len(batch))
	for _, item := range batch {
		msg := item.KafkaMsg
		if msg == nil || msg.TopicPartition.Topic == nil {
			continue
		}
		topic := *msg.TopicPartition.Topic
		refs = append(refs, telemetry.KafkaRef{
			Topic:   topic,
			Headers: msg.Headers,
			VIP:     topic == kafkaPlatform.TopicVIP,
		})
	}
	return refs
}
