package accounting

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sms-gateway-arvan/internal/cache"
	kafka2 "sms-gateway-arvan/internal/platform/kafka"
	"sms-gateway-arvan/internal/platform/telemetry"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/redis/go-redis/v9"
)

type CompensationPayload struct {
	IdempHash string `json:"idemp_hash"`
	TenantID  string `json:"tenant_id"`
	Cost      int64  `json:"cost"`
	Reason    string `json:"reason"`
}

type CompensationWorker struct {
	brokers       string
	groupID       string
	db            *sql.DB
	rdb           *redis.Client
	balanceClient *cache.BalanceClient
	consumer      *kafka.Consumer
}

func NewCompensationWorker(brokers, groupID string, db *sql.DB, rdb *redis.Client) *CompensationWorker {
	return &CompensationWorker{
		brokers:       brokers,
		groupID:       groupID,
		db:            db,
		rdb:           rdb,
		balanceClient: cache.NewBalanceClient(rdb),
	}
}

func (cw *CompensationWorker) Start(ctx context.Context) {
	config := &kafka.ConfigMap{
		"bootstrap.servers":  cw.brokers,
		"group.id":           cw.groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		log.Fatalf("failed to create compensation kafka consumer: %v", err)
	}
	cw.consumer = consumer
	defer func() { _ = cw.consumer.Close() }()

	if err = cw.consumer.SubscribeTopics([]string{kafka2.TopicCompensation}, nil); err != nil {
		log.Fatalf("failed to subscribe to compensation topic: %v", err)
	}

	log.Printf("compensation worker started, listening on topic: %s", kafka2.TopicCompensation)

	for {
		select {
		case <-ctx.Done():
			log.Println("stopping compensation worker...")
			return
		default:
			ev := cw.consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				if err := cw.consumeCompensation(ctx, e); err != nil {
					// خطا داخل consumeCompensation روی همان تریس ثبت شده است.
					continue
				}
				// کامیت دستی آفست پس از موفقیت قطعی
				if _, err := cw.consumer.CommitMessage(e); err != nil {
					log.Printf("[COMPENSATION-ERROR] failed to commit offset: %v", err)
				}

			case kafka.Error:
				log.Printf("compensation consumer error: %v", e)

			default:
			}
		}
	}
}

func (cw *CompensationWorker) consumeCompensation(ctx context.Context, msg *kafka.Message) (err error) {
	ctx, endTrace := telemetry.ContinueCompensation(ctx, msg.Headers)
	defer func() { endTrace(err) }()
	err = cw.processCompensation(ctx, msg)
	if err != nil {
		telemetry.Printf(ctx, "[COMPENSATION-ERROR] processing failed for offset %s: %v", msg.TopicPartition, err)
	}
	return err
}

// processCompensation هسته اصلی تصمیم‌گیری Saga
func (cw *CompensationWorker) processCompensation(ctx context.Context, msg *kafka.Message) error {
	var payload CompensationPayload
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		telemetry.Printf(ctx, "[COMPENSATION-CORRUPT] invalid payload: %v", err)
		return nil // پیام خراب را رد می‌کنیم تا بلاک نشود
	}

	txCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()

	tx, err := cw.db.BeginTx(txCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	metadata, _ := json.Marshal(map[string]interface{}{
		"reason":       payload.Reason,
		"compensated":  true,
		"processed_at": time.Now().UTC(),
	})

	cost := payload.Cost
	if cost <= 0 {
		cost = cache.DefaultCost
	}
	costStr := strconv.FormatInt(cost, 10)

	var eventType string
	err = tx.QueryRowContext(txCtx, `
		SELECT event_type FROM outbox_events WHERE idempotency_key = $1 FOR UPDATE
	`, payload.IdempHash).Scan(&eventType)
	if errors.Is(err, sql.ErrNoRows) {
		// سناریوی ۱: هنوز outbox نیست. tombstone می‌گذاریم تا حسابداری بعداً کسر نکند.
		_, err = tx.ExecContext(txCtx, `
			INSERT INTO outbox_events (event_type, event_version, payload, idempotency_key)
			VALUES ('message.rejected', 1, $1, $2)
		`, metadata, payload.IdempHash)
		if err != nil {
			return fmt.Errorf("failed to insert outbox tombstone: %w", err)
		}
		if err = tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit tombstone tx: %w", err)
		}
		if err = cw.balanceClient.Rollback(ctx, payload.TenantID, payload.IdempHash); err != nil {
			return fmt.Errorf("failed to rollback redis reservation: %w", err)
		}
		telemetry.Printf(ctx, "[SAGA-ROLLBACK-SUCCESS] rollback reservation of %s completed for tenant %s (Idemp: %s)", costStr, payload.TenantID, payload.IdempHash)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to read outbox: %w", err)
	}

	// سناریوی ۲: قبلاً accepted شده. فقط همین گذار پول پستگرس را برمی‌گرداند.
	if eventType == "message.accepted" {
		res, err := tx.ExecContext(txCtx, `
			UPDATE outbox_events
			SET event_type = 'message.refunded', updated_at = NOW()
			WHERE idempotency_key = $1 AND event_type = 'message.accepted'
		`, payload.IdempHash)
		if err != nil {
			return fmt.Errorf("failed to mark refunded: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows == 0 {
			return fmt.Errorf("outbox status changed while refunding")
		}
		_, err = tx.ExecContext(txCtx, `
			UPDATE wallet
			SET available_balance = available_balance + $1,
			    updated_at = NOW()
			WHERE customer_id = $2
		`, cost, payload.TenantID)
		if err != nil {
			return fmt.Errorf("failed to refund balance in db: %w", err)
		}
		if err = tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit refund tx: %w", err)
		}
		if err = cw.balanceClient.RefundAvailable(ctx, payload.TenantID, payload.IdempHash, cost); err != nil {
			return fmt.Errorf("failed to refund r edis available: %w", err)
		}
		telemetry.Printf(ctx, "[SAGA-REFUND-SUCCESS] refund of %s completed for tenant %s (Idemp: %s)", costStr, payload.TenantID, payload.IdempHash)
		return nil
	}

	// قبلاً refund شده. پستگرس دوباره دست نمی‌خورد. ردیس اگر جا مانده باشد یک بار شارژ می‌شود.
	if eventType == "message.refunded" {
		if err = cw.balanceClient.RefundAvailable(ctx, payload.TenantID, payload.IdempHash, cost); err != nil {
			return fmt.Errorf("failed to refund redis available: %w", err)
		}
		telemetry.Printf(ctx, "[COMPENSATION-IDEMPOTENT] message %s was already refunded", payload.IdempHash)
		return nil
	}

	// rejected. رزرو ردیس اگر هنوز processing باشد برمی‌گردد.
	if err = cw.balanceClient.Rollback(ctx, payload.TenantID, payload.IdempHash); err != nil {
		return fmt.Errorf("failed to rollback redis reservation: %w", err)
	}
	telemetry.Printf(ctx, "[COMPENSATION-IDEMPOTENT] message %s was already processed with status: %s", payload.IdempHash, eventType)
	return nil
}
