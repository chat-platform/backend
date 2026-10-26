package config

import (
	"testing"
	"time"
)

func setValidCoreAPIEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"LISTEN_ADDR", "GRPC_ADDR", "METRICS_ADDR", "POSTGRES_DSN", "REDIS_ADDR", "DEV_OTP_CODE",
		"SESSION_TTL", "MEDIA_URL_TTL", "MEDIA_S3_ENDPOINT", "MEDIA_S3_REGION",
		"JWT_SECRET", "MEDIA_S3_BUCKET", "MEDIA_S3_ACCESS_KEY", "MEDIA_S3_SECRET_KEY",
	} {
		t.Setenv(key, "")
	}
	t.Setenv("JWT_SECRET", "this-is-a-test-secret-with-more-than-32-bytes")
	t.Setenv("MEDIA_S3_BUCKET", "chat-media")
	t.Setenv("MEDIA_S3_ACCESS_KEY", "access")
	t.Setenv("MEDIA_S3_SECRET_KEY", "secret")
}

func TestFromEnvDefaults(t *testing.T) {
	setValidCoreAPIEnv(t)
	for _, key := range []string{"LISTEN_ADDR", "GRPC_ADDR", "METRICS_ADDR", "POSTGRES_DSN", "REDIS_ADDR", "DEV_OTP_CODE", "SESSION_TTL", "MEDIA_URL_TTL", "MEDIA_S3_ENDPOINT", "MEDIA_S3_REGION"} {
		t.Setenv(key, "")
	}
	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ListenAddr != ":8081" || cfg.GRPCAddr != ":50052" || cfg.MetricsAddr != "127.0.0.1:9091" || cfg.PostgresDSN != defaultPostgresDSN || cfg.RedisAddr != "localhost:6379" || cfg.DevOTPCode != "000000" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.SessionTTL != 24*time.Hour || cfg.Media.URLTTL != 5*time.Minute || cfg.Media.Region != "us-east-1" || cfg.Media.Endpoint != "https://s3.us-east-1.amazonaws.com" {
		t.Fatalf("unexpected duration or media defaults: %+v", cfg)
	}
}

func TestFromEnvOverrides(t *testing.T) {
	setValidCoreAPIEnv(t)
	t.Setenv("LISTEN_ADDR", ":9001")
	t.Setenv("GRPC_ADDR", ":9002")
	t.Setenv("METRICS_ADDR", "127.0.0.1:9191")
	t.Setenv("POSTGRES_DSN", "postgres://test/db")
	t.Setenv("REDIS_ADDR", "redis:6380")
	t.Setenv("DEV_OTP_CODE", "123456")
	t.Setenv("SESSION_TTL", "48h")
	t.Setenv("MEDIA_URL_TTL", "30m")
	t.Setenv("MEDIA_S3_REGION", "eu-west-1")
	t.Setenv("MEDIA_S3_ENDPOINT", "http://minio:9000")
	t.Setenv("MEDIA_S3_BUCKET", "test-bucket")
	t.Setenv("MEDIA_S3_ACCESS_KEY", "test-access")
	t.Setenv("MEDIA_S3_SECRET_KEY", "test-secret")
	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ListenAddr != ":9001" || cfg.GRPCAddr != ":9002" || cfg.MetricsAddr != "127.0.0.1:9191" || cfg.PostgresDSN != "postgres://test/db" || cfg.RedisAddr != "redis:6380" || cfg.DevOTPCode != "123456" || cfg.SessionTTL != 48*time.Hour {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if cfg.Media.URLTTL != 30*time.Minute || cfg.Media.Endpoint != "http://minio:9000" || cfg.Media.Region != "eu-west-1" || cfg.Media.Bucket != "test-bucket" || cfg.Media.AccessKey != "test-access" || cfg.Media.SecretKey != "test-secret" {
		t.Fatalf("unexpected media config: %+v", cfg.Media)
	}
}

func TestFromEnvRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		set  func(*testing.T)
	}{
		{name: "short JWT secret", set: func(t *testing.T) { t.Setenv("JWT_SECRET", "short") }},
		{name: "invalid session duration", set: func(t *testing.T) { t.Setenv("SESSION_TTL", "bad") }},
		{name: "nonpositive session duration", set: func(t *testing.T) { t.Setenv("SESSION_TTL", "0s") }},
		{name: "invalid media duration", set: func(t *testing.T) { t.Setenv("MEDIA_URL_TTL", "bad") }},
		{name: "media duration too short", set: func(t *testing.T) { t.Setenv("MEDIA_URL_TTL", "500ms") }},
		{name: "media duration too long", set: func(t *testing.T) { t.Setenv("MEDIA_URL_TTL", "169h") }},
		{name: "missing bucket", set: func(t *testing.T) { t.Setenv("MEDIA_S3_BUCKET", "") }},
		{name: "missing access key", set: func(t *testing.T) { t.Setenv("MEDIA_S3_ACCESS_KEY", "") }},
		{name: "missing secret key", set: func(t *testing.T) { t.Setenv("MEDIA_S3_SECRET_KEY", "") }},
		{name: "invalid media endpoint", set: func(t *testing.T) { t.Setenv("MEDIA_S3_ENDPOINT", "file:///tmp/media") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setValidCoreAPIEnv(t)
			test.set(t)
			if _, err := FromEnv(); err == nil {
				t.Fatal("FromEnv() unexpectedly succeeded")
			}
		})
	}
}
