package main

import (
	"fmt"
	"os"

	"github.com/chat-platform/backend/services/ws-gateway/internal/config"
)

func main() {
	cfg := config.FromEnv()
	fmt.Fprintf(os.Stderr, "ws-gateway skeleton (phase 0); listen=%s node_id=%s\n", cfg.ListenAddr, cfg.NodeID)
}
