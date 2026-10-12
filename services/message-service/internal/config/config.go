package config

import (
	"log"
	"os"
)

type Config struct {
	GRPCAddr    string
	PostgresDSN string
}

func FromEnv() Config {
	return Config{
		GRPCAddr:    getenv("GRPC_ADDR", ":50051"),
		PostgresDSN: getenv("POSTGRES_DSN", "postgres://messenger:messenger@localhost:5432/messenger?sslmode=disable"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	log.Printf("Environment variable %s is not set; using default value", key)
	return fallback
}
