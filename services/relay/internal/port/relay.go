package port

import (
	"context"
	"github.com/chat-platform/backend/services/relay/internal/domain"
)

type RouteFunc func(context.Context, domain.OutboxEvent) (domain.Route, error)
type OutboxRepository interface {
	Drain(context.Context, int, RouteFunc) (int, error)
}
type ConnectionRegistry interface {
	Lookup(context.Context, string) (domain.Connection, bool, error)
}
type StreamPublisher interface {
	Publish(context.Context, domain.Connection, domain.OutboxEvent) error
}
