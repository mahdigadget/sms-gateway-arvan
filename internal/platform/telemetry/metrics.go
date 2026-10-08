package telemetry

import (
	"context"
	"errors"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// مرزهای تأخیر برای مسیر VIP که در حد چند میلی‌ثانیه حساس است.
var latencyBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

// Signals چهار شمارندهٔ Four Golden Signals برای درخواست HTTP.
type Signals struct {
	traffic  metric.Int64Counter
	latency  metric.Float64Histogram
	errors   metric.Int64Counter
	inflight metric.Int64UpDownCounter
}

// Setup متریک، تریس و لاگ را با آدرس OTEL از env آماده می‌کند.
// لاگ داخل تریس، TraceID و SpanID استاندارد رکورد OTLP را دارد.
// نمونه‌بردار SDK همه‌چیز را می‌فرستد. نگهداری tail در کالکتور انجام می‌شود.
// قطع بودن کالکتور جلوی بالا آمدن سرویس را نمی‌گیرد.
func Setup(ctx context.Context, endpoint, serviceName string) (*Signals, func(context.Context) error, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(attribute.String("service.name", serviceName)),
	)
	if err != nil {
		return nil, nil, err
	}

	stopTrace, err := startTracing(ctx, endpoint, res)
	if err != nil {
		return nil, nil, err
	}
	stopLogs, err := startLogging(ctx, endpoint, serviceName, res)
	if err != nil {
		_ = stopTrace(ctx)
		return nil, nil, err
	}

	exporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(endpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		_ = stopTrace(ctx)
		_ = stopLogs(ctx)
		return nil, nil, err
	}

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(15*time.Second))),
	)
	otel.SetMeterProvider(provider)

	signals, err := newSignals(provider.Meter(serviceName))
	if err != nil {
		_ = provider.Shutdown(ctx)
		_ = stopTrace(ctx)
		_ = stopLogs(ctx)
		return nil, nil, err
	}
	stop := func(ctx context.Context) error {
		return errors.Join(stopTrace(ctx), provider.Shutdown(ctx), stopLogs(ctx))
	}
	return signals, stop, nil
}

func startTracing(ctx context.Context, endpoint string, res *resource.Resource) (func(context.Context) error, error) {
	setPropagator()
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}

func newSignals(meter metric.Meter) (*Signals, error) {
	traffic, err := meter.Int64Counter(
		"http.server.requests",
		metric.WithDescription("Traffic: completed HTTP requests"),
	)
	if err != nil {
		return nil, err
	}

	latency, err := meter.Float64Histogram(
		"http.server.request.duration",
		metric.WithDescription("Latency: HTTP request duration"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(latencyBuckets...),
	)
	if err != nil {
		return nil, err
	}

	errors, err := meter.Int64Counter(
		"http.server.request.errors",
		metric.WithDescription("Errors: HTTP responses with status 500 or higher"),
	)
	if err != nil {
		return nil, err
	}

	inflight, err := meter.Int64UpDownCounter(
		"http.server.active_requests",
		metric.WithDescription("Saturation: HTTP requests currently in flight"),
	)
	if err != nil {
		return nil, err
	}

	return &Signals{
		traffic:  traffic,
		latency:  latency,
		errors:   errors,
		inflight: inflight,
	}, nil
}

// Middleware دور هر درخواست چهار سیگنال را می‌نویسد و بدنهٔ هندلر را عوض نمی‌کند.
func (s *Signals) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := context.WithoutCancel(c.Request.Context())
		s.inflight.Add(ctx, 1)
		started := time.Now()

		c.Next()

		s.inflight.Add(ctx, -1)
		attrs := attributes(c)
		s.traffic.Add(ctx, 1, attrs)
		s.latency.Record(ctx, time.Since(started).Seconds(), attrs)
		if c.Writer.Status() >= 500 {
			s.errors.Add(ctx, 1, attrs)
		}
	}
}

func attributes(c *gin.Context) metric.MeasurementOption {
	route := c.FullPath()
	if route == "" {
		route = "unmatched"
	}
	return metric.WithAttributes(
		attribute.String("http.request.method", c.Request.Method),
		attribute.String("http.route", route),
		attribute.Int("http.response.status_code", c.Writer.Status()),
	)
}
