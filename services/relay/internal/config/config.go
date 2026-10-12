package config

import "os"

type Config struct {
	PostgresDSN string
	RedisAddr   string
}

func FromEnv() Config {
	return Config{
		PostgresDSN: getenv("POSTGRES_DSN", "postgres://messenger:messenger@localhost:5432/messenger?sslmode=disable"),
		RedisAddr:   getenv("REDIS_ADDR", "localhost:6379"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
