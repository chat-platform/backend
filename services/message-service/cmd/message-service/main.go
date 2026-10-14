package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	grpcserver "github.com/chat-platform/backend/services/message-service/internal/adapter/inbound/grpc"
	"github.com/chat-platform/backend/services/message-service/internal/adapter/outbound/postgres"
	"github.com/chat-platform/backend/services/message-service/internal/app"
	"github.com/chat-platform/backend/services/message-service/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/bufbuild/protovalidate-go"
	"google.golang.org/grpc"
)

func main() {
	cfg := config.FromEnv()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("ping postgres: %v", err)
	}

	listener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Fatalf("listen on %s: %v", cfg.GRPCAddr, err)
	}
	server := grpc.NewServer()
	validator, err := protovalidate.New()
	if err != nil {
		log.Fatalf("initialize request validator: %v", err)
	}
	grpcserver.Register(server, app.NewSendMessage(postgres.NewOutbox(pool)), validator)
	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()
	log.Printf("message-service listening on %s", cfg.GRPCAddr)
	if err := server.Serve(listener); err != nil {
		log.Fatalf("serve gRPC: %v", err)
	}
}
