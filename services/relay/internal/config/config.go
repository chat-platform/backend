package config

import (
	"log"
	"os"
)

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
	log.Printf("Environment variable %s is not set; using default value", key)
	return fallback
}
