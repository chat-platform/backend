.PHONY: proto up down tidy\
	running

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

running:
	CompileDaemon \
		-build="go build -o /tmp/message-service ./services/message-service/cmd/message-service" \
		-command="env LOG_LEVEL=debug /tmp/message-service" \
		-directory="." \
		-exclude-dir=.git