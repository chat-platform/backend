package port

import (
	"context"

	"github.com/chat-platform/backend/services/message-service/internal/domain"
)

type ChatRepository interface {
	FindMessageEvent(context.Context, domain.SendMessage) (domain.MessageAck, bool, error)
	IsAuthorizedToSend(context.Context, string, string) (bool, error)
	ResolveMessageRecipients(context.Context, string) ([]domain.RecipientDevice, error)
	EnqueueMessageEvent(context.Context, domain.SendMessage, []domain.RecipientDevice) (domain.MessageAck, error)
}
