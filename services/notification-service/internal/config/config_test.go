package config

import (
	"testing"
	"time"
)

func clearNotificationEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{"POSTGRES_DSN", "REDIS_ADDR", "GOOGLE_CLOUD_PROJECT", "NOTIFICATION_COALESCE_WINDOW"} {
		t.Setenv(key, "")
	}
}

func TestFromEnvDefaults(t *testing.T) {
	clearNotificationEnv(t)
	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PostgresDSN != defaultPostgresDSN || cfg.RedisAddr != "localhost:6379" || cfg.GoogleCloudProject != "" || cfg.NotificationCoalesceWindow != 5*time.Second {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestFromEnvOverrides(t *testing.T) {
	clearNotificationEnv(t)
	t.Setenv("POSTGRES_DSN", "postgres://test/db")
	t.Setenv("REDIS_ADDR", "redis:6380")
	t.Setenv("GOOGLE_CLOUD_PROJECT", "push-project")
	t.Setenv("NOTIFICATION_COALESCE_WINDOW", "250ms")
	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PostgresDSN != "postgres://test/db" || cfg.RedisAddr != "redis:6380" || cfg.GoogleCloudProject != "push-project" || cfg.NotificationCoalesceWindow != 250*time.Millisecond {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestFromEnvRejectsInvalidCoalesceWindow(t *testing.T) {
	for _, value := range []string{"invalid", "0s", "-1s"} {
		t.Run(value, func(t *testing.T) {
			clearNotificationEnv(t)
			t.Setenv("NOTIFICATION_COALESCE_WINDOW", value)
			if _, err := FromEnv(); err == nil {
				t.Fatalf("FromEnv() accepted NOTIFICATION_COALESCE_WINDOW=%q", value)
			}
		})
	}
}
