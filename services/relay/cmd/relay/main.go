package main

import (
	"fmt"
	"os"

	"github.com/chat-platform/backend/services/relay/internal/config"
)

func main() {
	cfg := config.FromEnv()
	fmt.Fprintf(os.Stderr, "relay skeleton (phase 0); postgres configured=%t redis=%s\n", cfg.PostgresDSN != "", cfg.RedisAddr)
}
