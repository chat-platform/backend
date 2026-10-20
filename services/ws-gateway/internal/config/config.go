package config

import (
	"context"
	"os"

	"github.com/chat-platform/backend/shared/pkg/logging"
)

type Config struct {
	ListenAddr         string
	RedisAddr          string
	MessageServiceAddr string
	CoreAPIAddr        string
	NodeID             string
	Path               string
	JWTSecret          string
}

func FromEnv(logger logging.Logger) Config {
	return Config{
		ListenAddr:         getenv(logger, "LISTEN_ADDR", ":8080"),
		RedisAddr:          getenv(logger, "REDIS_ADDR", "localhost:6379"),
		MessageServiceAddr: getenv(logger, "MESSAGE_SERVICE_ADDR", "localhost:50051"),
		CoreAPIAddr:        getenv(logger, "CORE_API_GRPC_ADDR", "localhost:50052"),
		NodeID:             getenv(logger, "NODE_ID", "ws-1"),
		Path:               getenv(logger, "WS_PATH", "/ws"),
		JWTSecret:          os.Getenv("JWT_SECRET"),
	}
}

func getenv(logger logging.Logger, key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	logger.Info(context.Background(), "environment variable is not set; using default value", "variable", key)
	return fallback
}
