package main

import (
	"fmt"
	"os"

	"github.com/chat-platform/backend/services/message-service/internal/config"
)

func main() {
	cfg := config.FromEnv()
	fmt.Fprintf(os.Stderr, "message-service skeleton (phase 0); grpc_addr=%s\n", cfg.GRPCAddr)
}
