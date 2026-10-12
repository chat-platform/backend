package config

import "os"

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
	return fallback
}
