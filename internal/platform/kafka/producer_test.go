package kafka

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestGeneratePartitionKey(t *testing.T) {
	if got := string(GeneratePartitionKey("tenant-a", false)); got != "tenant-a" {
		t.Fatalf("normal key = %q", got)
	}

	got := string(GeneratePartitionKey("tenant-a", true))
	salt, ok := strings.CutPrefix(got, "tenant-a:")
	if !ok {
		t.Fatalf("whale key = %q", got)
	}
	n, err := strconv.Atoi(salt)
	if err != nil || n < 0 || n >= WhaleSaltBuckets {
		t.Fatalf("salt = %q", salt)
	}
}

func TestAwaitDelivery(t *testing.T) {
	topic := TopicVIP
	okCh := make(chan kafka.Event, 1)
	okCh <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic}}
	if err := awaitDelivery(okCh); err != nil {
		t.Fatal(err)
	}

	failCh := make(chan kafka.Event, 1)
	failCh <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Error: errors.New("broker")}}
	if err := awaitDelivery(failCh); !errors.Is(err, ErrDeliveryFailed) {
		t.Fatalf("broker error: %v", err)
	}

	otherCh := make(chan kafka.Event, 1)
	otherCh <- kafka.Stats{}
	if err := awaitDelivery(otherCh); !errors.Is(err, ErrDeliveryFailed) {
		t.Fatalf("non-message event: %v", err)
	}
}

func TestProduceStopsWhenClientIsGone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := (&ProducerPool{}).ProduceVIP(ctx, VIPMessagePayload{TenantID: "t"}, false)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("vip: %v", err)
	}
	err = (&ProducerPool{}).ProduceNormal(ctx, NormalMessagePayload{TenantID: "t"}, false)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("normal: %v", err)
	}
}
