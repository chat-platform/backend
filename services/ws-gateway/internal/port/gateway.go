package port

import (
	"context"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
)

type MessageService interface {
	SendMessage(context.Context, domain.SendMessage) (domain.MessageAck, error)
	MarkDelivered(context.Context, string, string, int64) error
	MarkRead(context.Context, string, string, string, string, string, int64) error
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
