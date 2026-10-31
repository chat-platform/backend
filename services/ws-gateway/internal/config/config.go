package config

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/chat-platform/backend/shared/pkg/logging"
)

const HeartbeatInterval = 20 * time.Second

type Config struct {
	ListenAddr         string
	RedisAddr          string
	MessageServiceAddr string
	CoreAPIAddr        string
	MetricsAddr        string
	NodeID             string
	Path               string
	AllowedOrigins     []string
	JWTSecret          string
}

func FromEnv(logger logging.Logger) Config {
	return Config{
		ListenAddr:         getenv(logger, "LISTEN_ADDR", ":8080"),
		RedisAddr:          getenv(logger, "REDIS_ADDR", "localhost:6379"),
		MessageServiceAddr: getenv(logger, "MESSAGE_SERVICE_ADDR", "localhost:50051"),
		CoreAPIAddr:        getenv(logger, "CORE_API_GRPC_ADDR", "localhost:50052"),
		MetricsAddr:        getenv(logger, "METRICS_ADDR", "127.0.0.1:9094"),
		NodeID:             getenv(logger, "NODE_ID", "ws-1"),
		Path:               getenv(logger, "WS_PATH", "/ws"),
		AllowedOrigins:     listEnv(logger, "CORS_ALLOWED_ORIGINS", "http://localhost:5173"),
		JWTSecret:          os.Getenv("JWT_SECRET"),
	}
}

func listEnv(logger logging.Logger, key, fallback string) []string {
	parts := strings.Split(getenv(logger, key, fallback), ",")
	origins := make([]string, 0, len(parts))
	for _, part := range parts {
		if origin := strings.TrimSpace(part); origin != "" {
			origins = append(origins, origin)
		}
	}
	return origins
}

func getenv(logger logging.Logger, key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	logger.Info(context.Background(), "environment variable is not set; using default value", "variable", key)
	return fallback
}
