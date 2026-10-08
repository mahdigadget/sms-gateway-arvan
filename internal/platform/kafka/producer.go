package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"sms-gateway-arvan/internal/platform/telemetry"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	TopicVIP          = "sms.vip"
	TopicNormal       = "sms.normal"
	TopicCompensation = "sms.accounting.compensation"

	// تعداد باکت ها توزیع بار برای نهنگ‌ها
	WhaleSaltBuckets = 16
)

var (
	ErrDeliveryFailed = errors.New("kafka delivery failed")
	ErrProducerClosed = errors.New("kafka producer closed")
)

type VIPMessagePayload struct {
	TenantID    string    `json:"tenant_id"`
	Destination string    `json:"destination"`
	Sender      string    `json:"sender"`
	Body        string    `json:"body"`
	DeadlineAt  int64     `json:"deadline_at"` // ثانیه از زمان دریافت
	CreatedAt   time.Time `json:"created_at"`
	IdempHash   string    `json:"idemp_hash"`
}

type NormalMessagePayload struct {
	TenantID    string    `json:"tenant_id"`
	Destination string    `json:"destination"`
	Sender      string    `json:"sender"`
	Body        string    `json:"body"`
	CreatedAt   time.Time `json:"created_at"`
	IdempHash   string    `json:"idemp_hash"`
}

type ProducerPool struct {
	VipProducer    *kafka.Producer
	NormalProducer *kafka.Producer
}

// GeneratePartitionKey: برای نهنگ‌ها سالت اضافه می‌کند تا بار روی چند پارتیشن پخش شود
func GeneratePartitionKey(tenantID string, isWhale bool) []byte {
	if !isWhale {
		return []byte(tenantID)
	}

	// تولید سالت تصادفی ایمن
	n := rand.IntN(WhaleSaltBuckets)
	return []byte(fmt.Sprintf("%s:%d", tenantID, n))
}

// ProduceVIP: ارسال با تضمین حداکثری و منتظر ماندن برای ack
func (p *ProducerPool) ProduceVIP(ctx context.Context, payload VIPMessagePayload, isWhale bool) error {
	// created_at را عوض نمی‌کنیم؛ مبدأ مهلت همان زمان دریافت API است.
	// Timestamp پیام برای بک‌پرشر ۹۰ms است و جدا از deadline_at می‌ماند.
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload error: %w", err)
	}
	// اگر کلاینت قبل از صف شدن رفته، اصلاً Produce نمی‌زنیم تا رول‌بک امن باشد
	if err = ctx.Err(); err != nil {
		return err
	}
	deliverChan := make(chan kafka.Event, 1)
	key := GeneratePartitionKey(payload.TenantID, isWhale)
	topic := TopicVIP

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:       key,
		Value:     data,
		Timestamp: time.Now().UTC(),
	}
	done := telemetry.TrackSend(ctx, topic, payload.IdempHash, &msg.Headers)
	if err = p.VipProducer.Produce(msg, deliverChan); err != nil {
		err = fmt.Errorf("produce vip error: %w", err)
		done(err)
		return err
	}
	// پیام داخل صف پرودیوسر است. rollback فقط با جواب خود بروکر.
	// کانال را نمی‌بندیم؛ پرودیوسر روی آن می‌نویسد و close یعنی panic.
	err = awaitDelivery(deliverChan)
	done(err)
	return err
}

// ProduceNormal: ارسال پیام‌های عادی با بهینه‌سازی توان عملیاتی
func (p *ProducerPool) ProduceNormal(ctx context.Context, payload NormalMessagePayload, isWhale bool) error {
	payload.CreatedAt = time.Now().UTC()
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload error: %w", err)
	}
	if err = ctx.Err(); err != nil {
		return err
	}

	deliveryChan := make(chan kafka.Event, 1)

	key := GeneratePartitionKey(payload.TenantID, isWhale)
	topic := TopicNormal

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          data,
		Timestamp:      payload.CreatedAt,
	}
	done := telemetry.TrackSend(ctx, topic, payload.IdempHash, &msg.Headers)
	if err = p.NormalProducer.Produce(msg, deliveryChan); err != nil {
		err = fmt.Errorf("produce normal error: %w", err)
		done(err)
		return err
	}
	err = awaitDelivery(deliveryChan)
	done(err)
	return err
}

// awaitDelivery تا گزارش بروکر صبر می‌کند. context کلاینت این‌جا دخالت نمی‌کند.
func awaitDelivery(ch <-chan kafka.Event) error {
	e := <-ch
	m, ok := e.(*kafka.Message)
	if !ok || m.TopicPartition.Error != nil {
		if m != nil && m.TopicPartition.Error != nil {
			return fmt.Errorf("%w: %v", ErrDeliveryFailed, m.TopicPartition.Error)
		}
		return ErrDeliveryFailed
	}
	return nil
}
