//go:build integration

// این تست‌ها ردیس، کافکا، پستگرس و کلیک‌هوس واقعی را بالا می‌آورند.
// اجرا: go test -tags integration ./internal/integration
package integration

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"sms-gateway-arvan/internal/cache"
	"sms-gateway-arvan/internal/handler"
	kafkaPlatform "sms-gateway-arvan/internal/platform/kafka"
	"sms-gateway-arvan/internal/report"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gin-gonic/gin"
	"github.com/moby/moby/api/types/container"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"golang.org/x/sync/errgroup"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	_ "github.com/lib/pq"
)

const (
	redisAddr   = "127.0.0.1:16379"
	postgresDSN = "postgres://postgres:1234@127.0.0.1:15432/sms_gateway?sslmode=disable"
	kafkaBroker = "127.0.0.1:19092"
	clickAddr   = "127.0.0.1:9000"
)

var (
	rdb      *redis.Client
	db       *sql.DB
	ch       clickhouse.Conn
	brokers  string
	store    *report.Store
	router   *gin.Engine
	balances *cache.BalanceClient
	shutdown func()
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	var err error
	shutdown, err = boot(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration stack: %v\n", err)
		if shutdown != nil {
			shutdown()
		}
		os.Exit(1)
	}

	code := m.Run()
	shutdown()
	os.Exit(code)
}

func boot(ctx context.Context) (func(), error) {
	var redisC, postgresC, kafkaC, clickC testcontainers.Container
	stop := func() {
		termCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		for _, c := range []testcontainers.Container{redisC, postgresC, kafkaC, clickC} {
			if c != nil {
				_ = c.Terminate(termCtx)
			}
		}
		if rdb != nil {
			_ = rdb.Close()
		}
		if db != nil {
			_ = db.Close()
		}
	}

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		var err error
		redisC, err = testcontainers.Run(groupCtx, "redis:8.10.2",
			hostNetwork(),
			testcontainers.WithCmd("redis-server", "--port", "16379", "--bind", "127.0.0.1"),
			testcontainers.WithWaitStrategyAndDeadline(time.Minute, wait.ForExec([]string{"redis-cli", "-p", "16379", "ping"})),
		)
		return err
	})
	group.Go(func() error {
		var err error
		postgresC, err = testcontainers.Run(groupCtx, "postgres:18.6",
			hostNetwork(),
			testcontainers.WithEnv(map[string]string{
				"POSTGRES_USER":     "postgres",
				"POSTGRES_PASSWORD": "1234",
				"POSTGRES_DB":       "sms_gateway",
				"PGPORT":            "15432",
			}),
			testcontainers.WithFiles(testcontainers.ContainerFile{
				HostFilePath:      repoPath("deploy/postgres/init.sql"),
				ContainerFilePath: "/docker-entrypoint-initdb.d/01-schema.sql",
				FileMode:          0o644,
			}),
			testcontainers.WithWaitStrategyAndDeadline(2*time.Minute,
				wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			),
		)
		return err
	})
	group.Go(func() error {
		var err error
		kafkaC, err = testcontainers.Run(groupCtx, "apache/kafka:4.3.1",
			hostNetwork(),
			testcontainers.WithEnv(map[string]string{
				"CLUSTER_ID":                                     "4L6g3nShT-eMCtK--X86sw",
				"KAFKA_NODE_ID":                                  "1",
				"KAFKA_PROCESS_ROLES":                            "broker,controller",
				"KAFKA_LISTENERS":                                "PLAINTEXT://127.0.0.1:19092,CONTROLLER://127.0.0.1:19094",
				"KAFKA_ADVERTISED_LISTENERS":                     "PLAINTEXT://127.0.0.1:19092",
				"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
				"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
				"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@127.0.0.1:19094",
				"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
				"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
				"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
				"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			}),
			testcontainers.WithWaitStrategyAndDeadline(2*time.Minute, wait.ForLog("Kafka Server started")),
		)
		return err
	})
	group.Go(func() error {
		var err error
		clickC, err = testcontainers.Run(groupCtx, "clickhouse/clickhouse-server:26.9.12.8",
			hostNetwork(),
			testcontainers.WithEnv(map[string]string{
				"CLICKHOUSE_DB":                        "sms",
				"CLICKHOUSE_USER":                      "default",
				"CLICKHOUSE_PASSWORD":                  "clickhouse",
				"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1",
			}),
			testcontainers.WithWaitStrategyAndDeadline(2*time.Minute,
				wait.ForExec([]string{"clickhouse-client", "--user", "default", "--password", "clickhouse", "--query", "SELECT 1"}),
			),
		)
		return err
	})
	if err := group.Wait(); err != nil {
		return stop, err
	}

	rdb = redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		return stop, fmt.Errorf("redis ping: %w", err)
	}

	var err error
	db, err = sql.Open("postgres", postgresDSN)
	if err != nil {
		return stop, fmt.Errorf("postgres open: %w", err)
	}
	if err = db.PingContext(ctx); err != nil {
		return stop, fmt.Errorf("postgres ping: %w", err)
	}

	brokers = kafkaBroker
	if err = createTopics(ctx); err != nil {
		return stop, fmt.Errorf("kafka topics: %w", err)
	}

	if err = waitClickHouse(ctx); err != nil {
		return stop, err
	}
	store = report.NewStore(ch)

	vip, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokers, "acks": "all", "linger.ms": 0})
	if err != nil {
		return stop, err
	}
	normal, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"acks":              "all",
		"linger.ms":         20,
		"compression.type":  "snappy",
	})
	if err != nil {
		vip.Close()
		return stop, err
	}
	balances = cache.NewBalanceClient(rdb)
	gin.SetMode(gin.TestMode)
	sms := handler.NewSMSHandler(balances, &kafkaPlatform.ProducerPool{VipProducer: vip, NormalProducer: normal})
	router = gin.New()
	router.POST("/api/v1/sms/vip", sms.SendVIP)
	router.POST("/api/v1/sms/normal", sms.SendNormal)

	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTracerProvider(sdktrace.NewTracerProvider())

	prev := stop
	stop = func() {
		vip.Close()
		normal.Close()
		_ = ch.Close()
		prev()
	}
	return stop, nil
}

func hostNetwork() testcontainers.CustomizeRequestOption {
	return testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
		hc.NetworkMode = "host"
	})
}

func waitClickHouse(ctx context.Context) error {
	var last error
	for {
		if err := waitPort(ctx, clickAddr); err != nil {
			return fmt.Errorf("clickhouse port: %w", err)
		}
		conn, err := report.Open(ctx, clickAddr, "sms", "default", "clickhouse")
		if err == nil {
			ch = conn
			if err = execClickHouseSchema(ctx); err != nil {
				_ = ch.Close()
				return err
			}
			return nil
		}
		last = err
		select {
		case <-ctx.Done():
			return fmt.Errorf("clickhouse open: %w", last)
		case <-time.After(300 * time.Millisecond):
		}
	}
}

func waitPort(ctx context.Context, addr string) error {
	var last error
	for {
		conn, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		last = err
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %s", ctx.Err(), last)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func createTopics(ctx context.Context) error {
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		return err
	}
	defer admin.Close()

	topics := []kafka.TopicSpecification{
		{Topic: kafkaPlatform.TopicVIP, NumPartitions: 1, ReplicationFactor: 1},
		{Topic: kafkaPlatform.TopicNormal, NumPartitions: 1, ReplicationFactor: 1},
		{Topic: kafkaPlatform.TopicCompensation, NumPartitions: 1, ReplicationFactor: 1},
	}
	results, err := admin.CreateTopics(ctx, topics)
	if err != nil {
		return err
	}
	for _, result := range results {
		if result.Error.Code() == kafka.ErrNoError || result.Error.Code() == kafka.ErrTopicAlreadyExists {
			continue
		}
		return result.Error
	}
	return nil
}

func execClickHouseSchema(ctx context.Context) error {
	raw, err := os.ReadFile(repoPath("internal/report/schema.sql"))
	if err != nil {
		return err
	}
	var cleaned strings.Builder
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		cleaned.WriteString(line)
		cleaned.WriteByte('\n')
	}
	for _, stmt := range strings.Split(cleaned.String(), ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if err = ch.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}
	return nil
}

func repoPath(rel string) string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", rel)
}

func seedRedis(t *testing.T, tenant string, available int) {
	t.Helper()
	err := rdb.HSet(context.Background(), "wallet:"+tenant, "available", available, "reserved", 0).Err()
	if err != nil {
		t.Fatal(err)
	}
}

func seedPostgres(t *testing.T, tenant string, available int) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO wallet (customer_id, available_balance) VALUES ($1, $2)`, tenant, available)
	if err != nil {
		t.Fatal(err)
	}
}

func redisBalance(t *testing.T, tenant string) (available, reserved int) {
	t.Helper()
	vals, err := rdb.HMGet(context.Background(), "wallet:"+tenant, "available", "reserved").Result()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Sscan(fmt.Sprint(vals[0]), &available)
	fmt.Sscan(fmt.Sprint(vals[1]), &reserved)
	return available, reserved
}

func postgresBalance(t *testing.T, tenant string) int {
	t.Helper()
	var balance int
	err := db.QueryRow(`SELECT available_balance FROM wallet WHERE customer_id = $1`, tenant).Scan(&balance)
	if err != nil {
		t.Fatal(err)
	}
	return balance
}
