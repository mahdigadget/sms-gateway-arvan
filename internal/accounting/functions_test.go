package accounting

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	kafkaPlatform "sms-gateway-arvan/internal/platform/kafka"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestSplitIdempKey(t *testing.T) {
	tenant, hash := splitIdempKey("idemp:11111111-1111-1111-1111-111111111111:abc123")
	if tenant != "11111111-1111-1111-1111-111111111111" || hash != "abc123" {
		t.Fatalf("tenant=%q hash=%q", tenant, hash)
	}

	for _, key := range []string{"", "wallet:tenant", "idemp:only-one", "idemp:", "idemp:tenant:"} {
		tenant, hash = splitIdempKey(key)
		if tenant != "" || hash != "" {
			t.Fatalf("%q -> %q %q", key, tenant, hash)
		}
	}
}

func TestParseMessage(t *testing.T) {
	created := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	tenant := "11111111-1111-1111-1111-111111111111"
	body, err := json.Marshal(KafkaMessagePayload{
		TenantID:    tenant,
		Destination: "09120000000",
		Sender:      "1000",
		Body:        "code",
		DeadlineAt:  30,
		CreatedAt:   created,
		IdempHash:   "hash",
	})
	if err != nil {
		t.Fatal(err)
	}
	topic := kafkaPlatform.TopicVIP
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic}, Value: body}

	record, err := (&Worker{}).parseMessage(msg)
	if err != nil {
		t.Fatal(err)
	}
	if record.Kind != "otp" || record.Cost != 1 || record.IdempotencyKey != "hash" {
		t.Fatalf("%+v", record)
	}
	if record.DeadlineAt == nil || !record.DeadlineAt.Equal(created.Add(30*time.Second)) {
		t.Fatalf("deadline = %v", record.DeadlineAt)
	}
	if string(record.RequestHash) != "hash" || record.KafkaMsg != msg {
		t.Fatal("payload was not kept on the record")
	}

	body, err = json.Marshal(KafkaMessagePayload{TenantID: tenant, CreatedAt: created, IdempHash: "n"})
	if err != nil {
		t.Fatal(err)
	}
	msg.Value = body
	record, err = (&Worker{}).parseMessage(msg)
	if err != nil {
		t.Fatal(err)
	}
	if record.Kind != "normal" || record.DeadlineAt != nil {
		t.Fatalf("normal record: %+v", record)
	}

	msg.Value = []byte("{")
	if _, err = (&Worker{}).parseMessage(msg); err == nil {
		t.Fatal("broken json was accepted")
	}
	msg.Value = []byte(`{"tenant_id":"not-a-uuid"}`)
	if _, err = (&Worker{}).parseMessage(msg); err == nil || !strings.Contains(err.Error(), "invalid tenant UUID") {
		t.Fatalf("tenant error: %v", err)
	}
}

func TestKafkaRefs(t *testing.T) {
	vip := kafkaPlatform.TopicVIP
	normal := kafkaPlatform.TopicNormal
	batch := []*ConsumedRecord{
		{KafkaMsg: &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &vip}, Headers: []kafka.Header{{Key: "traceparent", Value: []byte("vip")}}}},
		{KafkaMsg: &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &normal}}},
		{KafkaMsg: nil},
		{KafkaMsg: &kafka.Message{}},
	}

	refs := kafkaRefs(batch)
	if len(refs) != 2 {
		t.Fatalf("refs = %#v", refs)
	}
	if !refs[0].VIP || refs[0].Topic != vip || string(refs[0].Headers[0].Value) != "vip" {
		t.Fatalf("vip ref = %#v", refs[0])
	}
	if refs[1].VIP || refs[1].Topic != normal {
		t.Fatalf("normal ref = %#v", refs[1])
	}
}
