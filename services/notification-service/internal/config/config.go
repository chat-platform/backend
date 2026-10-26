package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

const defaultPostgresDSN = "postgres://messenger:messenger@localhost:5432/messenger?sslmode=disable"

type Config struct {
	PostgresDSN                string
	RedisAddr                  string
	GoogleCloudProject         string
	NotificationCoalesceWindow time.Duration
	MetricsAddr                string
}

func FromEnv() (Config, error) {
	coalesceWindow := 5 * time.Second
	if value := strings.TrimSpace(os.Getenv("NOTIFICATION_COALESCE_WINDOW")); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed <= 0 {
			return Config{}, fmt.Errorf("NOTIFICATION_COALESCE_WINDOW must be a positive duration: %q", value)
		}
		coalesceWindow = parsed
	}
	return Config{
		PostgresDSN:                stringEnv("POSTGRES_DSN", defaultPostgresDSN),
		RedisAddr:                  stringEnv("REDIS_ADDR", "localhost:6379"),
		GoogleCloudProject:         strings.TrimSpace(os.Getenv("GOOGLE_CLOUD_PROJECT")),
		NotificationCoalesceWindow: coalesceWindow,
		MetricsAddr:                stringEnv("METRICS_ADDR", "127.0.0.1:9095"),
	}, nil
}

func stringEnv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
