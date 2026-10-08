package health

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	kafkaPlatform "sms-gateway-arvan/internal/platform/kafka"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

const checkTimeout = 2 * time.Second

// Checker وابستگی‌های حیاتی فرایند api را برای readiness می‌سنجد.
type Checker struct {
	redis    *redis.Client
	db       *sql.DB
	producer *kafka.Producer
}

func New(redisClient *redis.Client, db *sql.DB, producer *kafka.Producer) *Checker {
	return &Checker{redis: redisClient, db: db, producer: producer}
}

// Register مسیر GET /health را بدون نیاز به middlewareهای طلایی سیگنال می‌گذارد.
func Register(r gin.IRoutes, c *Checker) {
	r.GET("/health", c.Handle)
}

type checkResult struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type response struct {
	Status string                 `json:"status"`
	Checks map[string]checkResult `json:"checks"`
}

func (c *Checker) Handle(ctx *gin.Context) {
	out := c.Check(ctx.Request.Context())
	code := http.StatusOK
	if out.Status != "ok" {
		code = http.StatusServiceUnavailable
	}
	ctx.JSON(code, out)
}

func (c *Checker) Check(parent context.Context) response {
	ctx, cancel := context.WithTimeout(parent, checkTimeout)
	defer cancel()

	checks := map[string]checkResult{
		"redis":    c.pingRedis(ctx),
		"postgres": c.pingPostgres(ctx),
		"kafka":    c.pingKafka(),
	}

	status := "ok"
	for _, result := range checks {
		if result.Status != "ok" {
			status = "unavailable"
			break
		}
	}
	return response{Status: status, Checks: checks}
}

func (c *Checker) pingRedis(ctx context.Context) checkResult {
	if c.redis == nil {
		return fail("redis client is nil")
	}
	if err := c.redis.Ping(ctx).Err(); err != nil {
		return fail(err.Error())
	}
	return ok()
}

func (c *Checker) pingPostgres(ctx context.Context) checkResult {
	if c.db == nil {
		return fail("postgres client is nil")
	}
	if err := c.db.PingContext(ctx); err != nil {
		return fail(err.Error())
	}
	return ok()
}

func (c *Checker) pingKafka() checkResult {
	if c.producer == nil {
		return fail("kafka producer is nil")
	}
	meta, err := c.producer.GetMetadata(nil, true, int(checkTimeout/time.Millisecond))
	if err != nil {
		return fail(err.Error())
	}
	if len(meta.Brokers) == 0 {
		return fail("no kafka brokers in metadata")
	}
	for _, topic := range []string{
		kafkaPlatform.TopicVIP,
		kafkaPlatform.TopicNormal,
		kafkaPlatform.TopicCompensation,
	} {
		info, exists := meta.Topics[topic]
		if !exists {
			return fail("missing topic " + topic)
		}
		if info.Error.Code() != kafka.ErrNoError {
			return fail(topic + ": " + info.Error.String())
		}
		if len(info.Partitions) == 0 {
			return fail("topic has no partitions: " + topic)
		}
	}
	return ok()
}

func ok() checkResult { return checkResult{Status: "ok"} }

func fail(msg string) checkResult {
	return checkResult{Status: "unavailable", Error: msg}
}
