package port

import (
	"context"
	"github.com/chat-platform/backend/services/relay/internal/domain"
	"time"
)

type OutboxRepository interface {
	ClaimBatch(context.Context, string, int, time.Duration) ([]domain.OutboxEvent, error)
	ClaimRetryBatch(context.Context, string, int, time.Duration) ([]domain.OutboxEvent, error)
	MarkDispatched(context.Context, string, int64, domain.Route) (time.Duration, error)
	MarkRetryAttempt(context.Context, string, int64, time.Time) (bool, error)
	ReleaseRetryClaim(context.Context, string, int64, time.Time, time.Duration) error
}

type OutboxSnapshotter interface {
	Snapshot(context.Context) ([]domain.OutboxSnapshot, error)
}

type ConnectionRegistry interface {
	Lookup(context.Context, string) (domain.Connection, bool, error)
}
type StreamPublisher interface {
	Publish(context.Context, domain.Connection, domain.OutboxEvent) error
	PublishNotification(context.Context, domain.OutboxEvent) error
}
