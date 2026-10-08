//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"sms-gateway-arvan/internal/accounting"
	"sms-gateway-arvan/internal/cache"
	kafkaPlatform "sms-gateway-arvan/internal/platform/kafka"
	"sms-gateway-arvan/internal/report"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

func TestWalletScripts(t *testing.T) {
	tenant := uuid.NewString()
	seedRedis(t, tenant, 5)
	hash := "hold-1"

	if err := balances.Reserve(context.Background(), tenant, hash); err != nil {
		t.Fatal(err)
	}
	if available, reserved := redisBalance(t, tenant); available != 4 || reserved != 1 {
		t.Fatalf("after reserve available=%d reserved=%d", available, reserved)
	}
	if err := balances.Reserve(context.Background(), tenant, hash); !errors.Is(err, cache.ErrRequestInProgress) {
		t.Fatalf("second reserve: %v", err)
	}

	if err := balances.StoreResponse(context.Background(), tenant, hash, http.StatusAccepted, map[string]string{"status": "queued"}); err != nil {
		t.Fatal(err)
	}
	err := balances.Reserve(context.Background(), tenant, hash)
	var replay *cache.ReplayResponse
	if !errors.As(err, &replay) || replay.HTTPStatus != http.StatusAccepted {
		t.Fatalf("replay: %v", err)
	}

	poor := uuid.NewString()
	seedRedis(t, poor, 0)
	if err = balances.Reserve(context.Background(), poor, "x"); !errors.Is(err, cache.ErrInsufficientFund) {
		t.Fatalf("insufficient: %v", err)
	}
	if err = balances.Reserve(context.Background(), uuid.NewString(), "x"); !errors.Is(err, cache.ErrCacheNotReady) {
		t.Fatalf("missing wallet: %v", err)
	}

	rolled := uuid.NewString()
	seedRedis(t, rolled, 3)
	if err = balances.Reserve(context.Background(), rolled, "back"); err != nil {
		t.Fatal(err)
	}
	if err = balances.Rollback(context.Background(), rolled, "back"); err != nil {
		t.Fatal(err)
	}
	if available, reserved := redisBalance(t, rolled); available != 3 || reserved != 0 {
		t.Fatalf("after rollback available=%d reserved=%d", available, reserved)
	}

	paid := uuid.NewString()
	seedRedis(t, paid, 3)
	if err = balances.Reserve(context.Background(), paid, "done"); err != nil {
		t.Fatal(err)
	}
	if err = balances.Commit(context.Background(), paid, "done"); err != nil {
		t.Fatal(err)
	}
	if available, reserved := redisBalance(t, paid); available != 2 || reserved != 0 {
		t.Fatalf("after commit available=%d reserved=%d", available, reserved)
	}
	if err = balances.RefundAvailable(context.Background(), paid, "done", 1); err != nil {
		t.Fatal(err)
	}
	if available, reserved := redisBalance(t, paid); available != 3 || reserved != 0 {
		t.Fatalf("after refund available=%d reserved=%d", available, reserved)
	}
}

func TestSendVIPAndNormal(t *testing.T) {
	tenant := uuid.NewString()
	seedRedis(t, tenant, 2)
	seedPostgres(t, tenant, 2)

	vip := send(t, "/api/v1/sms/vip", fmt.Sprintf(`{
		"tenant_id": %q,
		"destination": "09120000000",
		"sender": "1000",
		"body": "vip-code",
		"deadline_at": 30
	}`, tenant))
	if vip.Code != http.StatusAccepted {
		t.Fatalf("vip status=%d body=%s", vip.Code, vip.Body.String())
	}
	if available, reserved := redisBalance(t, tenant); available != 1 || reserved != 1 {
		t.Fatalf("after vip available=%d reserved=%d", available, reserved)
	}
	msg := waitMessage(t, kafkaPlatform.TopicVIP, idempOf(t, vip.Body.Bytes()))
	if header(msg, "traceparent") == "" {
		t.Fatal("vip message has no traceparent")
	}

	again := send(t, "/api/v1/sms/vip", fmt.Sprintf(`{
		"tenant_id": %q,
		"destination": "09120000000",
		"sender": "1000",
		"body": "vip-code",
		"deadline_at": 30
	}`, tenant))
	if again.Code != http.StatusAccepted {
		t.Fatalf("replay status=%d body=%s", again.Code, again.Body.String())
	}
	if available, reserved := redisBalance(t, tenant); available != 1 || reserved != 1 {
		t.Fatalf("replay changed the wallet: available=%d reserved=%d", available, reserved)
	}

	normal := send(t, "/api/v1/sms/normal", fmt.Sprintf(`{
		"tenant_id": %q,
		"destination": "09121111111",
		"sender": "2000",
		"body": "normal-text"
	}`, tenant))
	if normal.Code != http.StatusAccepted {
		t.Fatalf("normal status=%d body=%s", normal.Code, normal.Body.String())
	}
	waitMessage(t, kafkaPlatform.TopicNormal, idempOf(t, normal.Body.Bytes()))
	if available, reserved := redisBalance(t, tenant); available != 0 || reserved != 2 {
		t.Fatalf("after normal available=%d reserved=%d", available, reserved)
	}

	poor := send(t, "/api/v1/sms/normal", fmt.Sprintf(`{
		"tenant_id": %q,
		"destination": "09122222222",
		"sender": "2000",
		"body": "no-credit"
	}`, tenant))
	if poor.Code != http.StatusPaymentRequired {
		t.Fatalf("insufficient status=%d body=%s", poor.Code, poor.Body.String())
	}

	missing := send(t, "/api/v1/sms/vip", fmt.Sprintf(`{
		"tenant_id": %q,
		"destination": "09120000000",
		"sender": "1000",
		"body": "cold",
		"deadline_at": 5
	}`, uuid.NewString()))
	if missing.Code != http.StatusServiceUnavailable {
		t.Fatalf("missing wallet status=%d body=%s", missing.Code, missing.Body.String())
	}
}

func TestAccountingAcceptsThenCompensationRefunds(t *testing.T) {
	tenant := uuid.NewString()
	seedRedis(t, tenant, 4)
	seedPostgres(t, tenant, 4)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go accounting.NewWorker(brokers, "acct-"+tenant, db, rdb).Start(ctx)
	go accounting.NewCompensationWorker(brokers, "comp-"+tenant, db, rdb).Start(ctx)

	res := send(t, "/api/v1/sms/vip", fmt.Sprintf(`{
		"tenant_id": %q,
		"destination": "09123333333",
		"sender": "1000",
		"body": "account-me",
		"deadline_at": 15
	}`, tenant))
	if res.Code != http.StatusAccepted {
		t.Fatalf("send status=%d body=%s", res.Code, res.Body.String())
	}
	idemp := idempOf(t, res.Body.Bytes())
	waitOutbox(t, idemp, "message.accepted")
	if available, reserved := redisBalance(t, tenant); available != 3 || reserved != 0 {
		t.Fatalf("after accept available=%d reserved=%d", available, reserved)
	}
	if got := postgresBalance(t, tenant); got != 3 {
		t.Fatalf("postgres balance = %d", got)
	}

	publishCompensation(t, tenant, idemp)
	waitOutbox(t, idemp, "message.refunded")
	if available, reserved := redisBalance(t, tenant); available != 4 || reserved != 0 {
		t.Fatalf("after refund available=%d reserved=%d", available, reserved)
	}
	if got := postgresBalance(t, tenant); got != 4 {
		t.Fatalf("postgres balance after refund = %d", got)
	}
}

func TestReportReadsClickHouse(t *testing.T) {
	tenant := uuid.NewString()
	customer := uuid.New()
	created := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	insertMessage(t, tenant, customer, "accepted-1", "message.accepted", 2, created)
	insertMessage(t, tenant, customer, "refunded-1", "message.refunded", 1, created.Add(time.Second))

	filter := report.Filter{
		TenantID: tenant,
		From:     created.Add(-time.Hour),
		To:       created.Add(time.Hour),
		Limit:    10,
	}
	summary, err := store.Summary(context.Background(), filter)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Total != 2 || summary.Accepted != 1 || summary.Refunded != 1 || summary.NetCost != 1 {
		t.Fatalf("%+v", summary)
	}

	page, err := store.List(context.Background(), filter)
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 2 || len(page.Items) != 2 || page.Items[0].Status != "refunded" {
		t.Fatalf("%+v", page)
	}

	other := filter
	other.TenantID = uuid.NewString()
	empty, err := store.Summary(context.Background(), other)
	if err != nil {
		t.Fatal(err)
	}
	if empty.Total != 0 || empty.NetCost != 0 {
		t.Fatalf("other tenant: %+v", empty)
	}
}

func send(t *testing.T, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func idempOf(t *testing.T, body []byte) string {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	idemp, _ := payload["idemp"].(string)
	if idemp == "" {
		t.Fatalf("response has no idemp: %s", body)
	}
	return idemp
}

func waitMessage(t *testing.T, topic, idemp string) *kafka.Message {
	t.Helper()
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           "read-" + uuid.NewString(),
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	if err = consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		ev := consumer.Poll(200)
		msg, ok := ev.(*kafka.Message)
		if !ok {
			continue
		}
		var payload struct {
			IdempHash string `json:"idemp_hash"`
		}
		if json.Unmarshal(msg.Value, &payload) == nil && payload.IdempHash == idemp {
			return msg
		}
	}
	t.Fatalf("message %s did not arrive on %s", idemp, topic)
	return nil
}

func header(msg *kafka.Message, key string) string {
	for _, item := range msg.Headers {
		if item.Key == key {
			return string(item.Value)
		}
	}
	return ""
}

func waitOutbox(t *testing.T, idemp, eventType string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var got string
	for time.Now().Before(deadline) {
		err := db.QueryRow(`SELECT event_type FROM outbox_events WHERE idempotency_key = $1`, idemp).Scan(&got)
		if err == nil && got == eventType {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("outbox %s = %q, want %s", idemp, got, eventType)
}

func publishCompensation(t *testing.T, tenant, idemp string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"idemp_hash": idemp,
		"tenant_id":  tenant,
		"cost":       1,
		"reason":     "TELECOM_DELIVERY_FAILED",
	})
	if err != nil {
		t.Fatal(err)
	}
	topic := kafkaPlatform.TopicCompensation
	delivery := make(chan kafka.Event, 1)
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokers, "acks": "all"})
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(tenant),
		Value:          body,
	}, delivery)
	if err != nil {
		t.Fatal(err)
	}
	ev := <-delivery
	msg, ok := ev.(*kafka.Message)
	if !ok || msg.TopicPartition.Error != nil {
		t.Fatalf("compensation publish: %v", ev)
	}
}

func insertMessage(t *testing.T, tenant string, customer uuid.UUID, key, eventType string, cost int64, created time.Time) {
	t.Helper()
	ctx := context.Background()
	err := ch.Exec(ctx, `
		INSERT INTO sms_messages (
			tenant_id, id, customer_id, idempotency_key, kind, recipient, sender, body, cost, event_type, created_at
		) VALUES (?, ?, ?, ?, 'otp', '09120000000', '1000', 'hello', ?, ?, ?)
	`, tenant, uuid.New(), customer, key, cost, eventType, created)
	if err != nil {
		t.Fatal(err)
	}
}
