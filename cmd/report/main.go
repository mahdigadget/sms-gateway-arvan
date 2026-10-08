package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sms-gateway-arvan/internal/config"
	"sms-gateway-arvan/internal/platform/telemetry"
	"sms-gateway-arvan/internal/report"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
)

// سرویس جدا برای گزارش. مسیر ارسال پیام در cmd/api دست نخورده می‌ماند.
func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Four Golden Signals: ترافیک، تأخیر، خطا و اشباع روی هر درخواست HTTP.
	// لاگ هم OTLP است و با TraceID و SpanID به همان تریس وصل می‌شود.
	signals, stopMetrics, err := telemetry.Setup(ctx, cfg.OTelEndpoint, "sms-gateway-report")
	if err != nil {
		log.Fatalf("failed to setup metrics: %v", err)
	}
	defer func() { _ = stopMetrics(context.Background()) }()

	conn, err := report.Open(ctx, cfg.ClickHouseAddr, cfg.ClickHouseDatabase, cfg.ClickHouseUser, cfg.ClickHousePassword)
	if err != nil {
		log.Fatalf("failed to connect to clickhouse: %v", err)
	}
	defer conn.Close()

	store := report.NewStore(conn)

	router := gin.New()
	router.Use(otelgin.Middleware("sms-gateway-report"))
	router.Use(signals.Middleware())
	router.Use(gin.Recovery())
	report.Register(router.Group("/api/v1/sms/report"), store)

	srv := &http.Server{
		Addr:         envOr("REPORT_HTTP_PORT", ":9096"),
		Handler:      router,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	go func() {
		log.Printf("sms report server is running on %s", srv.Addr)
		if err = srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutting down report server...")

	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err = srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("report server forced to shutdown: %v", err)
	}
	log.Println("report server exited cleanly")
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
