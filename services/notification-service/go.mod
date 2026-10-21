module github.com/chat-platform/backend/services/notification-service

go 1.26.0

require (
	github.com/chat-platform/backend/shared/pkg v0.0.0
	github.com/jackc/pgx/v5 v5.7.2
	github.com/redis/go-redis/v9 v9.7.0
)

replace github.com/chat-platform/backend/shared/pkg => ../../shared/pkg
