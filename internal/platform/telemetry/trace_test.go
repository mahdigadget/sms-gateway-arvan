package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

var spans *tracetest.SpanRecorder

func TestMain(m *testing.M) {
	spans = tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)))
	setPropagator()
	os.Exit(m.Run())
}

func TestLevelOf(t *testing.T) {
	if levelOf("[ACCOUNTING-ERROR] commit failed") != slog.LevelError {
		t.Fatal("error line was not an error")
	}
	if levelOf("[ACCOUNTING-WARN] pausing consumer") != slog.LevelWarn {
		t.Fatal("warn line was not a warning")
	}
	if levelOf("telecom worker started") != slog.LevelInfo {
		t.Fatal("start line was not info")
	}
}

func TestTrackSendInjectsTraceparent(t *testing.T) {
	spans.Reset()
	var headers []kafka.Header
	done := TrackSend(context.Background(), "sms.vip", "msg-1", &headers)
	done(nil)

	if headerValue(headers, "traceparent") == "" {
		t.Fatalf("headers = %#v", headers)
	}
	span := findSpan(t, "send sms.vip")
	if !span.SpanContext().TraceID().IsValid() {
		t.Fatal("missing trace id")
	}
	if attr(span, "messaging.system") != "kafka" || attr(span, "messaging.operation.name") != "send" {
		t.Fatalf("attributes = %v", span.Attributes())
	}
	if attr(span, "messaging.destination.name") != "sms.vip" || attr(span, "messaging.message.id") != "msg-1" {
		t.Fatalf("attributes = %v", span.Attributes())
	}
}

func TestContinueMessageKeepsParentTrace(t *testing.T) {
	spans.Reset()
	var headers []kafka.Header
	TrackSend(context.Background(), "sms.vip", "msg-1", &headers)(nil)
	parent := findSpan(t, "send sms.vip")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, end := ContinueMessage(ctx, "sms.vip", headers)
	cancel()
	if ctx.Err() == nil {
		t.Fatal("consumer context dropped cancellation")
	}
	end(errors.New("kafka offset not committed"))

	child := findSpan(t, "process sms.vip")
	if child.SpanContext().TraceID() != parent.SpanContext().TraceID() {
		t.Fatal("consumer span left the producer trace")
	}
	if child.Parent().SpanID() != parent.SpanContext().SpanID() {
		t.Fatal("consumer span is not a child of the producer span")
	}
	if child.Status().Code != codes.Error {
		t.Fatalf("status = %s", child.Status().Code)
	}
}

func TestObserveBatchLinksNormalAndParentsVIP(t *testing.T) {
	spans.Reset()
	vipHeaders := publish(t, "sms.vip")
	normalHeaders := publish(t, "sms.normal")
	vipSend := findSpan(t, "send sms.vip")
	normalSend := findSpan(t, "send sms.normal")

	_, end := ObserveBatch(context.Background(), []KafkaRef{
		{Topic: "sms.vip", Headers: vipHeaders, VIP: true},
		{Topic: "sms.normal", Headers: normalHeaders, VIP: false},
		{Topic: "sms.normal", Headers: nil, VIP: false},
	})
	end(errors.New("flush failed"))

	vip := findSpan(t, "process sms.vip")
	if vip.SpanContext().TraceID() != vipSend.SpanContext().TraceID() || vip.Parent().SpanID() != vipSend.SpanContext().SpanID() {
		t.Fatal("vip batch span was not a child of its own publish span")
	}
	if vip.Status().Code != codes.Error {
		t.Fatal("flush error did not mark the vip span")
	}

	batch := findSpan(t, "process sms.normal")
	if batch.SpanContext().TraceID() == normalSend.SpanContext().TraceID() {
		t.Fatal("normal batch reused one message as its parent trace")
	}
	if !hasLink(batch, normalSend.SpanContext()) {
		t.Fatal("normal batch has no link to the publish span")
	}
	if n := intAttr(batch, "messaging.batch.message_count"); n != 2 {
		t.Fatalf("message count = %d", n)
	}
	if batch.Status().Code != codes.Error {
		t.Fatal("flush error did not mark the batch span")
	}
}

func TestContinueCompensationUsesCompensateOperation(t *testing.T) {
	spans.Reset()
	headers := publish(t, "sms.vip")
	_, end := ContinueCompensation(context.Background(), headers)
	end(nil)

	span := findSpan(t, "compensate sms.accounting.compensation")
	if attr(span, "messaging.operation.name") != "compensate" {
		t.Fatalf("operation = %s", attr(span, "messaging.operation.name"))
	}
	if span.SpanContext().TraceID() != findSpan(t, "send sms.vip").SpanContext().TraceID() {
		t.Fatal("compensation left the message trace")
	}
}

func publish(t *testing.T, topic string) []kafka.Header {
	t.Helper()
	var headers []kafka.Header
	TrackSend(context.Background(), topic, "msg", &headers)(nil)
	return headers
}

func findSpan(t *testing.T, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	var found sdktrace.ReadOnlySpan
	for _, span := range spans.Ended() {
		if span.Name() == name {
			found = span
		}
	}
	if found == nil {
		t.Fatalf("span %q was not recorded", name)
	}
	return found
}

func headerValue(headers []kafka.Header, key string) string {
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

func attr(span sdktrace.ReadOnlySpan, key string) string {
	for _, item := range span.Attributes() {
		if string(item.Key) == key {
			return item.Value.AsString()
		}
	}
	return ""
}

func intAttr(span sdktrace.ReadOnlySpan, key string) int64 {
	for _, item := range span.Attributes() {
		if string(item.Key) == key {
			return item.Value.AsInt64()
		}
	}
	return 0
}

func hasLink(span sdktrace.ReadOnlySpan, target trace.SpanContext) bool {
	for _, link := range span.Links() {
		if link.SpanContext.TraceID() == target.TraceID() && link.SpanContext.SpanID() == target.SpanID() {
			return true
		}
	}
	return false
}
