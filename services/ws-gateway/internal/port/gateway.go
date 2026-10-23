package port

import (
	"context"
	"time"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
)

type MessageService interface {
	SendMessage(context.Context, domain.SendMessage) (domain.MessageAck, error)
	MarkDelivered(context.Context, string, string, int64) error
	MarkRead(context.Context, string, string, int64, int64) error
}

type ConnectionRegistry interface {
	Register(context.Context, string, domain.Connection) error
	Refresh(context.Context, string, domain.Connection) (bool, error)
	Unregister(context.Context, string, domain.Connection) error
}

type SessionAuthenticator interface {
	Authenticate(context.Context, string) (domain.Identity, error)
	IsSessionActive(context.Context, string) (bool, error)
}

type PresenceStore interface {
	RegisterConnection(context.Context, string, string) (bool, error)
	RefreshConnection(context.Context, string, string) (bool, error)
	RemoveConnection(context.Context, string, string) (bool, error)
	IsOnline(context.Context, string) (bool, error)
	Subscribe(context.Context, string, string) error
	Unsubscribe(context.Context, string, string) error
	UnsubscribeAll(context.Context, string) error
	CachedLastSeen(context.Context, string) (*time.Time, error)
	CacheLastSeen(context.Context, string, time.Time) error
	Publish(context.Context, domain.Presence) error
}

type PresenceCoreAPI interface {
	GetLastSeen(context.Context, string, string) (*time.Time, error)
	UpdateLastSeen(context.Context, string, time.Time) error
}
