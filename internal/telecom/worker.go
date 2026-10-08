package telecom

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	kafkaPlatform "sms-gateway-arvan/internal/platform/kafka"
	"sms-gateway-arvan/internal/platform/telemetry"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/redis/go-redis/v9"
)

// Δt=now()−msg.Timestamp

const (
	ThresholdHighLatency = 90 * time.Millisecond
	ThresholdLowLatency  = 50 * time.Millisecond
)

type Worker struct {
	brokers      string
	groupID      string
	consumer     *kafka.Consumer
	producer     *kafka.Producer // پرودیوسر برای رویدادهای جبرانی Saga
	redisClient  *redis.Client
	normalPaused bool
	mu           sync.Mutex
}

func NewWorker(brokers, groupID string, redisClient *redis.Client, producer *kafka.Producer) *Worker {
	return &Worker{
		brokers:     brokers,
		groupID:     groupID,
		redisClient: redisClient,
		producer:    producer,
	}
}
func (w *Worker) Start(ctx context.Context) {
	config := &kafka.ConfigMap{
		"bootstrap.servers":  w.brokers,
		"group.id":           w.groupID,
		"auto.offset.reset":  "earliest", // مثل حسابداری، ری‌استارت پیام را جا نگذارد
		"enable.auto.commit": false,      // کامیت دستی بعد از تحویل به اپراتور
		// fetch نباید برای پر شدن بچ بماند؛ سقفش باید زیر آستانهٔ ۵۰ms باشد
		"fetch.min.bytes":   1,
		"fetch.wait.max.ms": 10,
	}
	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		log.Fatalf("failed to create telecom consumer: %v", err)
	}
	w.consumer = consumer
	defer w.consumer.Close()

	err = w.consumer.SubscribeTopics([]string{kafkaPlatform.TopicVIP, kafkaPlatform.TopicNormal}, nil)
	if err != nil {
		log.Fatalf("failed to subscribe topics: %v", err)
	}

	log.Printf("telecom worker started, consuming from: %s, %s", kafkaPlatform.TopicVIP, kafkaPlatform.TopicNormal)

	for {
		select {
		case <-ctx.Done():
			log.Println("shutting down telecom worker")
			return
		default:
			ev := w.consumer.Poll(10)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				w.processMessage(ctx, e)

			case kafka.Error:
				log.Printf("telecom consumer error: %v", e)

			default:
				// سایر ایونت‌ها مانند Rebalance به صورت اتومات هندل می‌شوند
			}
		}
	}

}
func (w *Worker) processMessage(ctx context.Context, msg *kafka.Message) {
	topic := *msg.TopicPartition.Topic
	ctx, endTrace := telemetry.ContinueMessage(ctx, topic, msg.Headers)
	var err error
	defer func() { endTrace(err) }()

	var commit bool
	if topic == kafkaPlatform.TopicVIP {
		commit = w.handleVIP(ctx, msg)
	} else {
		commit = w.handleNormal(ctx, msg)
	}
	// خطای ردیس false برمی‌گرداند تا پیام در تاپیک بماند
	if !commit {
		err = fmt.Errorf("kafka offset not committed")
		return
	}
	if _, err = w.consumer.CommitMessage(msg); err != nil {
		telemetry.Printf(ctx, "failed to commit message: %v", err)
	}
}

func (w *Worker) handleVIP(ctx context.Context, msg *kafka.Message) bool {
	var payload struct {
		TenantID    string    `json:"tenant_id"`
		Destination string    `json:"destination"`
		Sender      string    `json:"sender"`
		Body        string    `json:"body"`
		DeadlineAt  int64     `json:"deadline_at"` // ثانیه از زمان دریافت API
		CreatedAt   time.Time `json:"created_at"`
		IdempHash   string    `json:"idemp_hash"`
	}
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		telemetry.Printf(ctx, "corrupted vip message payload: %v", err)
		return true
	}

	now := time.Now().UTC()

	// محاسبه Latency برای Backpressure. این ۹۰ms است و به deadline_at وصل نیست.
	latency := now.Sub(msg.Timestamp)
	w.adjustBackpressure(ctx, latency)

	// deadline_at ثانیه است. از زمان دریافت API گذشته باشد ارسال نمی‌کنیم.
	deadline := time.Duration(payload.DeadlineAt) * time.Second
	if now.Sub(payload.CreatedAt) > deadline {
		telemetry.Printf(ctx, "[TELECOM-VIP-DROPPED] deadline expired for tenant: %s, dest: %s (took %v, max: %v)",
			payload.TenantID, payload.Destination, now.Sub(payload.CreatedAt), deadline)
		// جبران وجه به علت اکسپایر شدن ددلاین
		if err := w.publishCompensation(ctx, payload.IdempHash, payload.TenantID); err != nil {
			return false
		}
		return true
	}

	// چک و رزرو ارسال در ریدیس جهت جلوگیری از ارسال دوبل حین کرش (Distributed Lock / Flag)
	dedupKey := "tele:dispatched:" + payload.IdempHash
	alreadySent, ok := w.reserveDispatch(ctx, dedupKey, payload.IdempHash)
	if !ok {
		return false
	}
	if alreadySent {
		return true
	}

	// ارسال به / Gateway تلکام
	err := w.sendToTelcoGateway(ctx, payload.IdempHash, payload.Destination, payload.Sender, payload.Body, true)
	if err != nil {
		// اگر کلید پاک نشود IN_FLIGHT می‌ماند و جبران را نمی‌فرستیم تا ارسال دوباره ممکن باشد
		if _, errDel := w.redisClient.Del(ctx, dedupKey).Result(); errDel != nil {
			telemetry.Printf(ctx, "[TELECOM-VIP-ERROR] failed to release dedup key: %v", errDel)
			return false
		}
		telemetry.Printf(ctx, "[TELECOM-VIP-ERROR] failed to send message: %v", err)
		// ارسال رویداد Saga Compensation به علت خطای اپراتور
		if err := w.publishCompensation(ctx, payload.IdempHash, payload.TenantID); err != nil {
			return false
		}
		return true
	}
	if err = w.redisClient.Set(ctx, dedupKey, "SENT", 24*time.Hour).Err(); err != nil {
		telemetry.Printf(ctx, "[TELECOM-VIP-ERROR] failed to mark sent: %v", err)
	}
	return true
}

func (w *Worker) handleNormal(ctx context.Context, msg *kafka.Message) bool {
	var payload struct {
		TenantID    string    `json:"tenant_id"`
		Destination string    `json:"destination"`
		Sender      string    `json:"sender"`
		Body        string    `json:"body"`
		CreatedAt   time.Time `json:"created_at"`
		IdempHash   string    `json:"idemp_hash"`
	}

	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		telemetry.Printf(ctx, "corrupted normal message payload: %v", err)
		return true
	}

	// چک و رزرو ارسال در ریدیس جهت جلوگیری از ارسال دوبل حین کرش (Distributed Lock / Flag)
	dedupKey := "tele:dispatched:" + payload.IdempHash
	alreadySent, ok := w.reserveDispatch(ctx, dedupKey, payload.IdempHash)
	if !ok {
		return false
	}
	if alreadySent {
		return true
	}

	// ارسال به / Gateway تلکام
	err := w.sendToTelcoGateway(ctx, payload.IdempHash, payload.Destination, payload.Sender, payload.Body, false)
	if err != nil {
		if _, errDel := w.redisClient.Del(ctx, dedupKey).Result(); errDel != nil {
			telemetry.Printf(ctx, "[TELECOM-NORMAL-ERROR] failed to release dedup key: %v", errDel)
			return false
		}
		telemetry.Printf(ctx, "[TELECOM-NORMAL-ERROR] failed to send message: %v", err)
		if err := w.publishCompensation(ctx, payload.IdempHash, payload.TenantID); err != nil {
			return false
		}
		return true
	}
	if err = w.redisClient.Set(ctx, dedupKey, "SENT", 24*time.Hour).Err(); err != nil {
		telemetry.Printf(ctx, "[TELECOM-NORMAL-ERROR] failed to mark sent: %v", err)
	}
	return true
}

// reserveDispatch کلید ارسال را می‌گیرد.
// alreadySent یعنی قبلاً SENT شده. ok=false یعنی ردیس خطا داده و آفست نباید برود.
// IN_FLIGHT یعنی تلاش قبلی وسط کار مرده و باید دوباره ارسال شود.
func (w *Worker) reserveDispatch(ctx context.Context, dedupKey, idempHash string) (alreadySent bool, ok bool) {
	acquired, err := w.redisClient.SetNX(ctx, dedupKey, "IN_FLIGHT", 24*time.Hour).Result()
	if err != nil {
		telemetry.Printf(ctx, "[TELECOM-VIP-ERROR] failed to acquire dedup key: %v", err)
		return false, false
	}
	if acquired {
		return false, true
	}

	state, err := w.redisClient.Get(ctx, dedupKey).Result()
	if err != nil {
		telemetry.Printf(ctx, "[TELECOM-VIP-ERROR] failed to read dedup key: %v", err)
		return false, false
	}
	if state == "SENT" {
		telemetry.Printf(ctx, "[TELECOM-DEDUP] skipped duplicate message: %s", idempHash)
		return true, true
	}
	telemetry.Printf(ctx, "[TELECOM-RETRY] in-flight message will be sent again: %s", idempHash)
	return false, true
}

// adjustBackpressure: مدیریت مکانیزم Pause/Resume تاپیک عادی
func (w *Worker) adjustBackpressure(ctx context.Context, vipLatency time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// سناریوی فشار بالا: استاپ کردن پیام‌های عادی
	if vipLatency > ThresholdHighLatency && !w.normalPaused {
		assignment, err := w.consumer.Assignment()
		if err != nil {
			telemetry.Printf(ctx, "failed to fetch assignment from kafka: %v", err)
			return
		}
		var normalPartitions []kafka.TopicPartition
		for _, tp := range assignment {
			if tp.Topic != nil && *tp.Topic == kafkaPlatform.TopicNormal {
				normalPartitions = append(normalPartitions, tp)
			}
		}
		if len(normalPartitions) > 0 {
			if err = w.consumer.Pause(normalPartitions); err != nil {
				telemetry.Printf(ctx, "failed to pause normal partitions: %v", err)
				return
			}
			w.normalPaused = true
			telemetry.Printf(ctx, "[BACKPRESSURE] VIP latency: %v (> %v). PAUSED normal partitions (%d partitions)",
				vipLatency, ThresholdHighLatency, len(normalPartitions))
		}
		return
	}

	// سناریوی بازگشت به حالت عادی: ادامه دادن پیام‌های عادی
	if vipLatency < ThresholdLowLatency && w.normalPaused {
		assignment, err := w.consumer.Assignment()
		if err != nil {
			return
		}

		var normalPartitions []kafka.TopicPartition
		for _, tp := range assignment {
			if tp.Topic != nil && *tp.Topic == kafkaPlatform.TopicNormal {
				normalPartitions = append(normalPartitions, tp)
			}
		}

		if len(normalPartitions) > 0 {
			if err := w.consumer.Resume(normalPartitions); err == nil {
				w.normalPaused = false
				telemetry.Printf(ctx, "[BACKPRESSURE] VIP latency normalized: %v (< %v). RESUMED normal partitions",
					vipLatency, ThresholdLowLatency)
			}
		}
	}
}

// publishCompensation رویداد جبرانی را می‌فرستد و تا ack بروکر صبر می‌کند.
// کانال را نمی‌بندیم؛ پرودیوسر روی آن می‌نویسد.
func (w *Worker) publishCompensation(ctx context.Context, idempHash, tenantID string) error {
	if w.producer == nil {
		telemetry.Println(ctx, "[TELECOM-COMPENSATION-ERROR] producer is nil")
		return fmt.Errorf("compensation producer is nil")
	}

	compPayload, err := json.Marshal(map[string]interface{}{
		"idemp_hash": idempHash,
		"tenant_id":  tenantID,
		"cost":       1,
		"reason":     "TELECOM_DELIVERY_FAILED",
	})
	if err != nil {
		telemetry.Printf(ctx, "failed to marshal compensation payload: %v", err)
		return err
	}

	topic := kafkaPlatform.TopicCompensation
	deliverChan := make(chan kafka.Event, 1)
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(tenantID),
		Value:          compPayload,
	}
	done := telemetry.TrackCompensateSend(ctx, idempHash, &message.Headers)
	defer func() { done(err) }()
	err = w.producer.Produce(message, deliverChan)
	if err != nil {
		telemetry.Printf(ctx, "[TELECOM-COMPENSATION-ERROR] failed to produce compensation event: %v", err)
		return err
	}

	timer := time.NewTimer(10 * time.Second)
	var e kafka.Event
	select {
	case e = <-deliverChan:
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	case <-timer.C:
		telemetry.Printf(ctx, "[TELECOM-COMPENSATION-ERROR] delivery timeout for message: %s", idempHash)
		err = fmt.Errorf("compensation delivery timeout")
		return err
	}
	m, ok := e.(*kafka.Message)
	if !ok || m.TopicPartition.Error != nil {
		if m != nil && m.TopicPartition.Error != nil {
			telemetry.Printf(ctx, "[TELECOM-COMPENSATION-ERROR] delivery failed: %v", m.TopicPartition.Error)
			err = m.TopicPartition.Error
			return err
		}
		telemetry.Printf(ctx, "[TELECOM-COMPENSATION-ERROR] delivery failed for message: %s", idempHash)
		err = fmt.Errorf("compensation delivery failed")
		return err
	}
	telemetry.Printf(ctx, "[TELECOM-COMPENSATION-SUCCESS] compensation event produced for message: %s", idempHash)
	return nil
}

func (w *Worker) sendToTelcoGateway(ctx context.Context, idempHash, dest, sender, body string, isVIP bool) error {
	// ماک خروجی تلکام
	if isVIP {
		fmt.Println(fmt.Sprintf("telecom worker sending VIP message to %s to %s and message is: %s", dest, sender, body))
	} else {
		fmt.Println(fmt.Sprintf("telecom worker sending Normal message to %s to %s and message is: %s", dest, sender, body))
	}
	return nil
}
