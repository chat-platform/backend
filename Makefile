.PHONY: proto up down tidy running run-all stop-all status-all logs-all logs-app

BUF ?= buf

proto:
	$(BUF) generate

up:
	docker compose up --build -d

down:
	docker compose down

tidy:
	go work sync
	cd shared/pkg && go mod tidy
	cd shared/gen && go mod tidy
	cd services/core-api && go mod tidy
	cd services/message-service && go mod tidy
	cd services/ws-gateway && go mod tidy
	cd services/relay && go mod tidy

running:
	CompileDaemon \
		-build="go build -o /tmp/message-service ./services/message-service/cmd/message-service" \
		-command="env LOG_LEVEL=debug /tmp/message-service" \
		-directory="." \
		-exclude-dir=.git

# Starts all Go services as background processes. PostgreSQL and Redis must
# already be running locally (this target does not use Docker Compose).
run-all:
	./scripts/local-services.sh start

stop-all:
	./scripts/local-services.sh stop

status-all:
	./scripts/local-services.sh status

logs-all:
	./scripts/local-services.sh logs

logs-app:
	./scripts/local-services.sh logs-without-relay
