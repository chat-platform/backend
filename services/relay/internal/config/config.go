package config

import (
	"context"
	"github.com/chat-platform/backend/shared/pkg/logging"
	"os"
	"strconv"
	"time"
)

type Config struct {
	PostgresDSN  string
	RedisAddr    string
	BatchSize    int
	PollInterval time.Duration
	ClaimLease   time.Duration
}

func FromEnv(logger logging.Logger) Config {
	batch := 100
	if value := os.Getenv("RELAY_BATCH_SIZE"); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			batch = parsed
		} else {
			logger.Warn(context.Background(), "invalid relay batch size; using default", "value", value)
		}
	}
	poll := 250 * time.Millisecond
	if value := os.Getenv("RELAY_POLL_INTERVAL"); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil && parsed > 0 {
			poll = parsed
		} else {
			logger.Warn(context.Background(), "invalid relay poll interval; using default", "value", value)
		}
	}
	lease := 30 * time.Second
	if value := os.Getenv("RELAY_CLAIM_LEASE"); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil && parsed > 0 {
			lease = parsed
		} else {
			logger.Warn(context.Background(), "invalid relay claim lease; using default", "value", value)
		}
	}
	return Config{
		PostgresDSN: getenv(logger, "POSTGRES_DSN", "postgres://messenger:messenger@localhost:5432/messenger?sslmode=disable"),
		RedisAddr:   getenv(logger, "REDIS_ADDR", "localhost:6379"), BatchSize: batch, PollInterval: poll, ClaimLease: lease,
	}
}

func getenv(logger logging.Logger, key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	logger.Info(context.Background(), "environment variable is not set; using default value", "variable", key)
	return fallback
}
