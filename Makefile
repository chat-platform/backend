.PHONY: proto up down tidy

PROTOC ?= protoc
GEN_MODULE := github.com/chat-platform/backend/shared/gen

proto:
	$(PROTOC) \
		--proto_path=shared/proto \
		--go_out=shared/gen \
		--go_opt=module=$(GEN_MODULE) \
		--go-grpc_out=shared/gen \
		--go-grpc_opt=module=$(GEN_MODULE) \
		shared/proto/message/v1/message.proto

up:
	docker compose up -d

down:
	docker compose down

tidy:
	go work sync
	cd shared/pkg && go mod tidy
	cd shared/gen && go mod tidy
	cd services/message-service && go mod tidy
	cd services/ws-gateway && go mod tidy
	cd services/relay && go mod tidy
