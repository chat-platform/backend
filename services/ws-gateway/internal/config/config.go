package config

import (
	"log"
	"os"
)

type Config struct {
	ListenAddr         string
	RedisAddr          string
	MessageServiceAddr string
	NodeID             string
}

func FromEnv() Config {
	return Config{
		ListenAddr:         getenv("LISTEN_ADDR", ":8080"),
		RedisAddr:          getenv("REDIS_ADDR", "localhost:6379"),
		MessageServiceAddr: getenv("MESSAGE_SERVICE_ADDR", "localhost:50051"),
		NodeID:             getenv("NODE_ID", "ws-1"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	log.Printf("Environment variable %s is not set; using default value", key)
	return fallback
}
