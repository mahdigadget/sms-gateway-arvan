package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sms-gateway-arvan/internal/accounting"
	"sms-gateway-arvan/internal/cache"
	"sms-gateway-arvan/internal/config"
	"sms-gateway-arvan/internal/handler"
	"sms-gateway-arvan/internal/health"
	kafkaPlatform "sms-gateway-arvan/internal/platform/kafka"
	"sms-gateway-arvan/internal/platform/telemetry"
	"sms-gateway-arvan/internal/telecom"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Four Golden Signals: ترافیک، تأخیر، خطا و اشباع روی هر درخواست HTTP.
	//Latency
	//Traffic
	//Errors
	//Saturation اشباع
	// تریس: قرارداد Messaging در هدر کافکا. بچ نرمال با Span Link، VIP با Parent-Child.
	// نگهداری tail در کالکتور: خطای کامل، جبران Saga، و VIP کندتر از ۹۰ms.
	// لاگ هم OTLP است و با TraceID و SpanID به همان تریس وصل می‌شود.
	signals, stopMetrics, err := telemetry.Setup(ctx, cfg.OTelEndpoint, "sms-gateway-api")
	if err != nil {
		log.Fatalf("failed to setup metrics: %v", err)
	}
	defer func() { _ = stopMetrics(context.Background()) }()

	//  اتصالات پایگاه‌های داده و کش
	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("failed to connect to redis: %v", err)
	}
	defer rdb.Close()

	db, err := sql.Open("postgres", cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("failed to open db connection: %v", err)
	}
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err = db.PingContext(ctx); err != nil {
		log.Fatalf("failed to ping postgres: %v", err)
	}
	defer db.Close()

	// ساخت Producer کافکا (تفکیک VIP و Normal)
	kafkaBrokers := cfg.KafkaBrokers

	// VIP Producer: حداکثر دوام و ارسال فوری
	vipProducer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBrokers,
		"acks":              "all",
		"linger.ms":         0,
	})
	if err != nil {
		log.Fatalf("failed to create vip kafka producer: %v", err)
	}
	defer vipProducer.Close()

	// Normal Producer: بچینگ با linger و acks=all
	normalProducer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBrokers,
		"acks":              "all",
		"linger.ms":         20,
		"compression.type":  "snappy", //Snappy• Characteristics: Optimized for high speed and reasonable compression efficiency. Similar to LZ4 but slightly slower compression speed.
		//• Best use case: High-throughput pipelines requiring reliable CPU efficiency
	})
	if err != nil {
		log.Fatalf("failed to create normal kafka producer: %v", err)
	}
	defer normalProducer.Close()

	producerPool := &kafkaPlatform.ProducerPool{
		VipProducer:    vipProducer,
		NormalProducer: normalProducer,
	}

	//  مقداردهی سرویس‌ها و هندلر
	balanceClient := cache.NewBalanceClient(rdb)
	smsHandler := handler.NewSMSHandler(balanceClient, producerPool)

	//  اجرای کانسیومرها در پس‌زمینه (Background Workers)
	// کانسیومر حسابداری (با مانیتورینگ سلامت DB و Pause/Resume)
	accountingWorker := accounting.NewWorker(kafkaBrokers, "accounting-group", db, rdb)
	go accountingWorker.Start(ctx)

	// کانسیومر جبران خسارت حسابداری (Saga Choreography: مدیریت Refund / Rollback بر اساس وضعیت دیتابیس)
	compensationWorker := accounting.NewCompensationWorker(kafkaBrokers, "accounting-compensation-group", db, rdb)
	go compensationWorker.Start(ctx)

	// رزروهای processing که بعد از قطع پروسه در کافکا ننشسته‌اند
	holdSweeper := accounting.NewHoldSweeper(db, rdb)
	go holdSweeper.Start(ctx)

	// کانسیومر تلکام (با مکانیزم Pause برای عادی‌ها بر اساس تاخیر VIP)
	telecomWorker := telecom.NewWorker(kafkaBrokers, "telecom-group", rdb, vipProducer)
	go telecomWorker.Start(ctx)

	router := gin.New()
	// /health قبل از متریکس/تریس تا probeها ترافیک Golden Signals را باد نکنند.
	health.Register(router, health.New(rdb, db, vipProducer))
	router.Use(otelgin.Middleware("sms-gateway-api"))
	router.Use(signals.Middleware())
	router.Use(gin.Recovery())

	v1 := router.Group("/api/v1/sms")
	{
		v1.POST("/vip", smsHandler.SendVIP)
		v1.POST("/normal", smsHandler.SendNormal)
	}

	// راه‌اندازی سرور HTTP به همراه Graceful Shutdown
	srv := &http.Server{
		Addr:         getEnv("HTTP_PORT", ":9095"),
		Handler:      router,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("server is running on %s", srv.Addr)
		if err = srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen error: %v", err)
		}
	}()

	// دریافت سیگنال خروج برای بستن تمیز منابع
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutting down server...")

	cancel() // توقف ورکرها و کانسومرها

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("server forced to shutdown: %v", err)
	}

	// خالی کردن بافر پرودیوسرها قبل از خروج
	vipProducer.Flush(5000)
	normalProducer.Flush(5000)

	log.Println("server exited cleanly")
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
