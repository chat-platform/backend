module github.com/chat-platform/backend/services/ws-gateway

go 1.25.0

require (
	github.com/chat-platform/backend/shared/gen v0.0.0
	github.com/chat-platform/backend/shared/pkg v0.0.0
	github.com/google/uuid v1.6.0
	github.com/gorilla/websocket v1.5.3
	github.com/redis/go-redis/v9 v9.7.0
	google.golang.org/grpc v1.84.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260706201446-f0a921348800 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/chat-platform/backend/shared/gen => ../../shared/gen

replace github.com/chat-platform/backend/shared/pkg => ../../shared/pkg
