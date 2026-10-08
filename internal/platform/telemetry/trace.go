package telemetry

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	messagingSystem = "kafka"

	opSend       = "send"
	opProcess    = "process"
	opCompensate = "compensate"
)

// KafkaRef یک پیام ورودی برای ربط دادن بچ حسابداری به تریس منتشرکننده.
type KafkaRef struct {
	Topic   string
	Headers []kafka.Header
	VIP     bool
}

// TrackSend اسپن publish را فرزند کانتکست فعلی می‌سازد و traceparent را در هدر کافکا می‌نویسد.
func TrackSend(ctx context.Context, topic, messageID string, headers *[]kafka.Header) func(error) {
	ctx, span := startMessaging(ctx, opSend, opSend, topic, messageID, trace.SpanKindProducer)
	inject(ctx, headers)
	return endSpan(span)
}

// TrackCompensateSend ادامهٔ همان تریس پیام است تا Saga در تریس اصلی بماند.
func TrackCompensateSend(ctx context.Context, messageID string, headers *[]kafka.Header) func(error) {
	ctx, span := startMessaging(ctx, opCompensate, opSend, "sms.accounting.compensation", messageID, trace.SpanKindProducer)
	inject(ctx, headers)
	return endSpan(span)
}

// ContinueMessage مصرف یک پیام را فرزند تریس منتشرکننده می‌کند. برای VIP و برای پیام تکی نرمال.
func ContinueMessage(ctx context.Context, topic string, headers []kafka.Header) (context.Context, func(error)) {
	ctx = withRemoteParent(ctx, headers)
	ctx, span := startMessaging(ctx, opProcess, opProcess, topic, "", trace.SpanKindConsumer)
	return ctx, endSpan(span)
}

// ContinueCompensation مصرف جبران را فرزند تریس همان پیام می‌کند.
func ContinueCompensation(ctx context.Context, headers []kafka.Header) (context.Context, func(error)) {
	ctx = withRemoteParent(ctx, headers)
	ctx, span := startMessaging(ctx, opCompensate, opProcess, "sms.accounting.compensation", "", trace.SpanKindConsumer)
	return ctx, endSpan(span)
}

// ObserveBatch بچ حسابداری را به تریس‌های اصلی وصل می‌کند.
// نرمال‌ها Span Link می‌گیرند تا بچ یک والد واحد نداشته باشد.
// هر VIP فرزند تریس خودش می‌ماند.
func ObserveBatch(ctx context.Context, refs []KafkaRef) (context.Context, func(error)) {
	var links []trace.Link
	var vip []trace.Span
	normals := 0

	for _, ref := range refs {
		parent := extract(ref.Headers)
		if ref.VIP {
			_, span := startMessaging(parent, opProcess, opProcess, ref.Topic, "", trace.SpanKindConsumer)
			vip = append(vip, span)
			continue
		}
		normals++
		if sc := trace.SpanContextFromContext(parent); sc.IsValid() {
			links = append(links, trace.Link{SpanContext: sc})
		}
	}

	var batch trace.Span
	if normals > 0 {
		ctx, batch = startMessaging(ctx, opProcess, opProcess, "sms.normal", "", trace.SpanKindConsumer, trace.WithLinks(links...))
		batch.SetAttributes(attribute.Int("messaging.batch.message_count", normals))
	}

	return ctx, func(err error) {
		if batch != nil {
			endSpan(batch)(err)
		}
		for _, span := range vip {
			endSpan(span)(err)
		}
	}
}

func startMessaging(ctx context.Context, operationName, operationType, topic, messageID string, kind trace.SpanKind, extra ...trace.SpanStartOption) (context.Context, trace.Span) {
	attrs := []attribute.KeyValue{
		attribute.String("messaging.system", messagingSystem),
		attribute.String("messaging.operation.name", operationName),
		attribute.String("messaging.operation.type", operationType),
		attribute.String("messaging.destination.name", topic),
	}
	if messageID != "" {
		attrs = append(attrs, attribute.String("messaging.message.id", messageID))
	}
	options := []trace.SpanStartOption{
		trace.WithSpanKind(kind),
		trace.WithAttributes(attrs...),
	}
	options = append(options, extra...)
	return otel.Tracer("sms-gateway").Start(ctx, operationName+" "+topic, options...)
}

func endSpan(span trace.Span) func(error) {
	return func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

func inject(ctx context.Context, headers *[]kafka.Header) {
	if headers == nil {
		return
	}
	otel.GetTextMapPropagator().Inject(ctx, kafkaCarrier{headers: headers})
}

func extract(headers []kafka.Header) context.Context {
	return otel.GetTextMapPropagator().Extract(context.Background(), kafkaCarrier{headers: &headers})
}

func withRemoteParent(ctx context.Context, headers []kafka.Header) context.Context {
	remote := trace.SpanContextFromContext(extract(headers))
	if !remote.IsValid() {
		return ctx
	}
	return trace.ContextWithRemoteSpanContext(ctx, remote)
}

type kafkaCarrier struct {
	headers *[]kafka.Header
}

func (c kafkaCarrier) Get(key string) string {
	if c.headers == nil {
		return ""
	}
	for _, header := range *c.headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

func (c kafkaCarrier) Set(key, value string) {
	if c.headers == nil {
		return
	}
	for i, header := range *c.headers {
		if header.Key == key {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, kafka.Header{Key: key, Value: []byte(value)})
}

func (c kafkaCarrier) Keys() []string {
	if c.headers == nil {
		return nil
	}
	keys := make([]string, len(*c.headers))
	for i, header := range *c.headers {
		keys[i] = header.Key
	}
	return keys
}

func setPropagator() {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
}
