package config

import (
	"io"
	"testing"

	"github.com/chat-platform/backend/shared/pkg/logging"
)

func TestFromEnvRelayWorkers(t *testing.T) {
	logger, err := logging.New(io.Discard, logging.Config{Level: "error"})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("defaults to two when unset", func(t *testing.T) {
		t.Setenv("RELAY_WORKERS", "")
		cfg, err := FromEnv(logger)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Workers != 2 {
			t.Fatalf("Workers=%d, want 2", cfg.Workers)
		}
	})

	t.Run("accepts a positive integer", func(t *testing.T) {
		t.Setenv("RELAY_WORKERS", "5")
		cfg, err := FromEnv(logger)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Workers != 5 {
			t.Fatalf("Workers=%d, want 5", cfg.Workers)
		}
	})

	for _, value := range []string{"0", "-1", "abc", "1.5"} {
		t.Run("rejects "+value, func(t *testing.T) {
			t.Setenv("RELAY_WORKERS", value)
			if _, err := FromEnv(logger); err == nil {
				t.Fatalf("FromEnv() accepted RELAY_WORKERS=%q", value)
			}
		})
	}
}
