package port

import (
	"context"
	"github.com/chat-platform/backend/services/relay/internal/domain"
	"time"
)

type OutboxRepository interface {
	ClaimBatch(context.Context, int, time.Duration) ([]domain.OutboxEvent, error)
	MarkDispatched(context.Context, int64, domain.Route) error
}
type ConnectionRegistry interface {
	Lookup(context.Context, string) (domain.Connection, bool, error)
}
type StreamPublisher interface {
	Publish(context.Context, domain.Connection, domain.OutboxEvent) error
}
