package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/bufbuild/protovalidate-go"
	grpcserver "github.com/chat-platform/backend/services/message-service/internal/adapter/inbound/grpc"
	"github.com/chat-platform/backend/services/message-service/internal/adapter/outbound/postgres"
	rediscache "github.com/chat-platform/backend/services/message-service/internal/adapter/outbound/redis"
	chatrepo "github.com/chat-platform/backend/services/message-service/internal/adapter/outbound/repository"
	"github.com/chat-platform/backend/services/message-service/internal/app"
	"github.com/chat-platform/backend/services/message-service/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
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

	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer redisClient.Close()

	listener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Fatalf("listen on %s: %v", cfg.GRPCAddr, err)
	}
	server := grpc.NewServer()
	validator, err := protovalidate.New()
	if err != nil {
		log.Fatalf("initialize request validator: %v", err)
	}
	postgresRepository := postgres.NewChatRepository(pool)
	ChatCache := rediscache.NewChatCache(redisClient)
	ChatCache.StartRepairWorker(ctx)
	chatRepository := chatrepo.NewCachedChatRepository(postgresRepository, ChatCache)
	grpcserver.Register(server, app.NewChatOperations(chatRepository), validator)
	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()
	log.Printf("message-service listening on %s", cfg.GRPCAddr)
	if err := server.Serve(listener); err != nil {
		log.Fatalf("serve gRPC: %v", err)
	}
}
