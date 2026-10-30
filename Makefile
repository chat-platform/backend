.PHONY: proto up down tidy running run-all stop-all status-all logs-all logs-app k8s-images-kind k8s-load-kind dockerhub-push dockerhub-push-all dockerhub-push-core-api dockerhub-push-message-service dockerhub-push-ws-gateway dockerhub-push-relay dockerhub-push-notification-service

BUF ?= buf
KIND_CLUSTER ?= kind
DOCKERHUB_USERNAME ?= abdulrahimom
DOCKERHUB_PLATFORM ?= linux/amd64
IMAGE_TAG ?= $(shell git rev-parse HEAD)
LOCAL_MEDIA_S3_ENDPOINT ?= http://localhost:9000
LOCAL_MEDIA_S3_REGION ?= us-east-1
LOCAL_MEDIA_S3_BUCKET ?= chat-media
LOCAL_MEDIA_S3_ACCESS_KEY ?= minioadmin
LOCAL_MEDIA_S3_SECRET_KEY ?= minioadmin
LOCAL_GOOGLE_APPLICATION_CREDENTIALS ?= $(CURDIR)/.local/firebase-service-account.json
LOCAL_COMPOSE = MEDIA_S3_ENDPOINT=$(LOCAL_MEDIA_S3_ENDPOINT) MEDIA_S3_REGION=$(LOCAL_MEDIA_S3_REGION) MEDIA_S3_BUCKET=$(LOCAL_MEDIA_S3_BUCKET) MEDIA_S3_ACCESS_KEY=$(LOCAL_MEDIA_S3_ACCESS_KEY) MEDIA_S3_SECRET_KEY=$(LOCAL_MEDIA_S3_SECRET_KEY) GOOGLE_APPLICATION_CREDENTIALS=$(LOCAL_GOOGLE_APPLICATION_CREDENTIALS) docker compose -f docker-compose.yml -f docker-compose.local.yml

proto:
	$(BUF) generate

up:
	@test -f "$(LOCAL_GOOGLE_APPLICATION_CREDENTIALS)" || { echo "Firebase service account not found: $(LOCAL_GOOGLE_APPLICATION_CREDENTIALS)" >&2; exit 1; }
	$(LOCAL_COMPOSE) up --build -d

down:
	$(LOCAL_COMPOSE) down

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

# Choose a backend service interactively, or push all five images.
# Run `docker login` first. IMAGE_TAG defaults to the current full Git SHA.
dockerhub-push:
	@printf '%s\n' \
		'Choose a backend image to push to Docker Hub:' \
		'  1) Core API' \
		'  2) Message Service' \
		'  3) WS Gateway' \
		'  4) Relay' \
		'  5) Notification Service' \
		'  6) All services' \
		'  q) Cancel'
	@printf 'Selection: '; read -r choice; \
	case "$$choice" in \
		1) $(MAKE) dockerhub-push-core-api ;; \
		2) $(MAKE) dockerhub-push-message-service ;; \
		3) $(MAKE) dockerhub-push-ws-gateway ;; \
		4) $(MAKE) dockerhub-push-relay ;; \
		5) $(MAKE) dockerhub-push-notification-service ;; \
		6) $(MAKE) dockerhub-push-all ;; \
		q|Q) printf 'Cancelled.\n' ;; \
		*) printf 'Invalid selection: %s\n' "$$choice" >&2; exit 2 ;; \
	esac

dockerhub-push-all: dockerhub-push-core-api dockerhub-push-message-service dockerhub-push-ws-gateway dockerhub-push-relay dockerhub-push-notification-service

dockerhub-push-core-api:
	docker buildx build --platform $(DOCKERHUB_PLATFORM) --file services/Dockerfile --build-arg SERVICE=core-api --tag $(DOCKERHUB_USERNAME)/messenger-core-api:$(IMAGE_TAG) --tag $(DOCKERHUB_USERNAME)/messenger-core-api:latest --push .

dockerhub-push-message-service:
	docker buildx build --platform $(DOCKERHUB_PLATFORM) --file services/Dockerfile --build-arg SERVICE=message-service --tag $(DOCKERHUB_USERNAME)/messenger-message-service:$(IMAGE_TAG) --tag $(DOCKERHUB_USERNAME)/messenger-message-service:latest --push .

dockerhub-push-ws-gateway:
	docker buildx build --platform $(DOCKERHUB_PLATFORM) --file services/Dockerfile --build-arg SERVICE=ws-gateway --tag $(DOCKERHUB_USERNAME)/messenger-ws-gateway:$(IMAGE_TAG) --tag $(DOCKERHUB_USERNAME)/messenger-ws-gateway:latest --push .

dockerhub-push-relay:
	docker buildx build --platform $(DOCKERHUB_PLATFORM) --file services/Dockerfile --build-arg SERVICE=relay --tag $(DOCKERHUB_USERNAME)/messenger-relay:$(IMAGE_TAG) --tag $(DOCKERHUB_USERNAME)/messenger-relay:latest --push .

dockerhub-push-notification-service:
	docker buildx build --platform $(DOCKERHUB_PLATFORM) --file services/Dockerfile --build-arg SERVICE=notification-service --tag $(DOCKERHUB_USERNAME)/messenger-notification-service:$(IMAGE_TAG) --tag $(DOCKERHUB_USERNAME)/messenger-notification-service:latest --push .
