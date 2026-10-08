package config

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Config struct {
	RedisAddr          string
	PostgresDSN        string
	KafkaBrokers       string
	ClickHouseAddr     string
	ClickHouseDatabase string
	ClickHouseUser     string
	ClickHousePassword string
	OTelEndpoint       string
}

// Load اول فایل .env را می‌خواند، بعد آدرس ردیس، پستگرس، کافکا، کلیک‌هوس و OTEL را برمی‌گرداند.
// اگر متغیری از قبل در محیط پروسه ست شده باشد، همان مقدار می‌ماند.
func Load() (Config, error) {
	if err := loadDotEnv(); err != nil {
		return Config{}, err
	}

	return Config{
		RedisAddr:          envOr("REDIS_ADDR", "localhost:6379"),
		PostgresDSN:        envOr("POSTGRES_DSN", "postgres://postgres:1234@localhost:5432/sms_gateway?sslmode=disable"),
		KafkaBrokers:       envOr("KAFKA_BROKERS", "localhost:9092"),
		ClickHouseAddr:     envOr("CLICKHOUSE_ADDR", "localhost:9000"),
		ClickHouseDatabase: envOr("CLICKHOUSE_DATABASE", "sms"),
		ClickHouseUser:     envOr("CLICKHOUSE_USER", "default"),
		ClickHousePassword: envOr("CLICKHOUSE_PASSWORD", ""),
		OTelEndpoint:       envOr("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
	}, nil
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func loadDotEnv() error {
	path, err := findEnvFile()
	if err != nil {
		return err
	}

	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open config file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return fmt.Errorf("%s:%d: expected KEY=VALUE", path, lineNo)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return fmt.Errorf("%s:%d: empty key", path, lineNo)
		}
		value = trimQuotes(strings.TrimSpace(value))

		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		if err = os.Setenv(key, value); err != nil {
			return fmt.Errorf("set %s: %w", key, err)
		}
	}
	if err = scanner.Err(); err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	return nil
}

func findEnvFile() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("working directory: %w", err)
	}

	for {
		candidate := filepath.Join(dir, ".env")
		info, statErr := os.Stat(candidate)
		if statErr == nil && !info.IsDir() {
			return candidate, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	return "", fmt.Errorf(".env not found")
}

func trimQuotes(value string) string {
	if len(value) < 2 {
		return value
	}
	if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'') {
		return value[1 : len(value)-1]
	}
	return value
}
