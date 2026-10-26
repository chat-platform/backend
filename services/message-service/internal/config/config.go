package config

import (
	"context"
	"os"

	"github.com/chat-platform/backend/shared/pkg/logging"
)

type Config struct {
	GRPCAddr    string
	MetricsAddr string
	PostgresDSN string
	RedisAddr   string
}

func FromEnv(logger logging.Logger) Config {
	return Config{
		GRPCAddr:    getenv(logger, "GRPC_ADDR", ":50051"),
		MetricsAddr: getenv(logger, "METRICS_ADDR", "127.0.0.1:9092"),
		PostgresDSN: getenv(logger, "POSTGRES_DSN", "postgres://messenger:messenger@localhost:5432/messenger?sslmode=disable"),
		RedisAddr:   getenv(logger, "REDIS_ADDR", "localhost:6379"),
	}
}

func getenv(logger logging.Logger, key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	logger.Info(context.Background(), "environment variable is not set; using default value", "variable", key)
	return fallback
}
