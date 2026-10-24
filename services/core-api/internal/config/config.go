package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"
)

const defaultPostgresDSN = "postgres://messenger:messenger@localhost:5432/messenger?sslmode=disable"

type MediaStorage struct {
	Endpoint  string
	Region    string
	Bucket    string
	AccessKey string
	SecretKey string
	URLTTL    time.Duration
}

type Config struct {
	ListenAddr  string
	GRPCAddr    string
	PostgresDSN string
	RedisAddr   string
	DevOTPCode  string
	JWTSecret   string
	SessionTTL  time.Duration
	Media       MediaStorage
}

func FromEnv() (Config, error) {
	region := stringEnv("MEDIA_S3_REGION", "us-east-1")
	endpoint := stringEnv("MEDIA_S3_ENDPOINT", "https://s3."+region+".amazonaws.com")
	sessionTTL, err := durationEnv("SESSION_TTL", 24*time.Hour)
	if err != nil {
		return Config{}, err
	}
	mediaURLTTL, err := durationEnv("MEDIA_URL_TTL", 5*time.Minute)
	if err != nil {
		return Config{}, err
	}
	if mediaURLTTL < time.Second || mediaURLTTL > 7*24*time.Hour {
		return Config{}, fmt.Errorf("MEDIA_URL_TTL must be between 1 second and 7 days")
	}

	jwtSecret := os.Getenv("JWT_SECRET")
	if len(jwtSecret) < 32 {
		return Config{}, fmt.Errorf("JWT_SECRET must contain at least 32 bytes")
	}
	media := MediaStorage{
		Endpoint:  endpoint,
		Region:    region,
		Bucket:    stringEnv("MEDIA_S3_BUCKET", ""),
		AccessKey: stringEnv("MEDIA_S3_ACCESS_KEY", ""),
		SecretKey: stringEnv("MEDIA_S3_SECRET_KEY", ""),
		URLTTL:    mediaURLTTL,
	}
	if media.Bucket == "" || media.AccessKey == "" || media.SecretKey == "" {
		return Config{}, fmt.Errorf("MEDIA_S3_BUCKET, MEDIA_S3_ACCESS_KEY, and MEDIA_S3_SECRET_KEY are required")
	}
	parsedEndpoint, err := url.Parse(endpoint)
	if err != nil || parsedEndpoint.Host == "" || (parsedEndpoint.Scheme != "http" && parsedEndpoint.Scheme != "https") {
		return Config{}, fmt.Errorf("MEDIA_S3_ENDPOINT must be an absolute HTTP(S) URL")
	}

	return Config{
		ListenAddr:  stringEnv("LISTEN_ADDR", ":8081"),
		GRPCAddr:    stringEnv("GRPC_ADDR", ":50052"),
		PostgresDSN: stringEnv("POSTGRES_DSN", defaultPostgresDSN),
		RedisAddr:   stringEnv("REDIS_ADDR", "localhost:6379"),
		DevOTPCode:  stringEnv("DEV_OTP_CODE", "000000"),
		JWTSecret:   jwtSecret,
		SessionTTL:  sessionTTL,
		Media:       media,
	}, nil
}

func durationEnv(key string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration: %q", key, value)
	}
	return parsed, nil
}

func stringEnv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
