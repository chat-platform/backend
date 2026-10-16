package port

import (
	"context"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
)

type MessageService interface {
	SendMessage(context.Context, domain.SendMessage) (domain.MessageAck, error)
}

type ConnectionRegistry interface {
	Register(context.Context, string, domain.Connection) error
	Refresh(context.Context, string, domain.Connection) (bool, error)
	Unregister(context.Context, string, domain.Connection) error
}
