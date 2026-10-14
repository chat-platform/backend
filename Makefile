.PHONY: proto up down tidy

BUF ?= buf

proto:
	$(BUF) generate

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
