package telemetry

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/trace"
)

// Printf همان log.Printf است، با این فرق که TraceID و SpanID را از ctx روی رکورد OTLP می‌نویسد.
func Printf(ctx context.Context, format string, args ...any) {
	emit(ctx, fmt.Sprintf(format, args...))
}

// Println همان log.Println است و مثل Printf به تریس جاری وصل می‌شود.
func Println(ctx context.Context, message string) {
	emit(ctx, message)
}

func emit(ctx context.Context, message string) {
	if ctx == nil {
		ctx = context.Background()
	}
	slog.Default().Log(ctx, levelOf(message), message)
}

func levelOf(message string) slog.Level {
	switch {
	case strings.Contains(message, "ERROR") || strings.Contains(message, "failed") || strings.Contains(message, "Failed"):
		return slog.LevelError
	case strings.Contains(message, "WARN"):
		return slog.LevelWarn
	default:
		return slog.LevelInfo
	}
}

func startLogging(ctx context.Context, endpoint, serviceName string, res *resource.Resource) (func(context.Context) error, error) {
	exporter, err := otlploggrpc.New(ctx,
		otlploggrpc.WithEndpoint(endpoint),
		otlploggrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}
	provider := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
	)
	stdout := &traceHandler{next: slog.NewTextHandler(os.Stdout, nil)}
	otelHandler := otelslog.NewHandler(serviceName, otelslog.WithLoggerProvider(provider))
	slog.SetDefault(slog.New(multiHandler{stdout, otelHandler}))
	// لاگ‌هایی که کانتکست تریس ندارند هم به کالکتور می‌روند، فقط TraceID ندارند.
	log.SetFlags(0)
	log.SetOutput(logBridge{})
	return provider.Shutdown, nil
}

// traceHandler شناسهٔ تریس را برای خروجی محلی کنار متن می‌گذارد.
// خود otelslog همان شناسه را روی فیلد استاندارد رکورد OTLP می‌نویسد.
type traceHandler struct {
	next slog.Handler
}

func (h *traceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *traceHandler) Handle(ctx context.Context, record slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.HasTraceID() {
		record = record.Clone()
		record.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.next.Handle(ctx, record)
}

func (h *traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceHandler{next: h.next.WithAttrs(attrs)}
}

func (h *traceHandler) WithGroup(name string) slog.Handler {
	return &traceHandler{next: h.next.WithGroup(name)}
}

type logBridge struct{}

func (logBridge) Write(p []byte) (int, error) {
	emit(context.Background(), strings.TrimSpace(string(p)))
	return len(p), nil
}

type multiHandler []slog.Handler

func (h multiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, next := range h {
		if next.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (h multiHandler) Handle(ctx context.Context, record slog.Record) error {
	var err error
	for _, next := range h {
		if nextErr := next.Handle(ctx, record.Clone()); nextErr != nil {
			err = nextErr
		}
	}
	return err
}

func (h multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	out := make(multiHandler, len(h))
	for i, next := range h {
		out[i] = next.WithAttrs(attrs)
	}
	return out
}

func (h multiHandler) WithGroup(name string) slog.Handler {
	out := make(multiHandler, len(h))
	for i, next := range h {
		out[i] = next.WithGroup(name)
	}
	return out
}
