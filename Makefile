.PHONY: proto up down tidy running run-all stop-all status-all logs-all logs-app k8s-images-kind k8s-load-kind

BUF ?= buf
KIND_CLUSTER ?= kind

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

# Build the Kubernetes service images with their local Kind tags.
k8s-images-kind:
	docker build -f services/Dockerfile --build-arg SERVICE=core-api -t messenger-core-api:local .
	docker build -f services/Dockerfile --build-arg SERVICE=message-service -t messenger-message-service:local .
	docker build -f services/Dockerfile --build-arg SERVICE=ws-gateway -t messenger-ws-gateway:local .
	docker build -f services/Dockerfile --build-arg SERVICE=relay -t messenger-relay:local .
	docker build -f services/Dockerfile --build-arg SERVICE=notification-service -t messenger-notification-service:local .
	docker build -f deployments/k8s/migration.Dockerfile -t messenger-migrate:local .

k8s-load-kind: k8s-images-kind
	kind load docker-image --name $(KIND_CLUSTER) messenger-core-api:local messenger-message-service:local messenger-ws-gateway:local messenger-relay:local messenger-notification-service:local messenger-migrate:local
