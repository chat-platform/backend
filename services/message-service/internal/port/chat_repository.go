package port

import (
	"context"

	"github.com/chat-platform/backend/services/message-service/internal/domain"
)

type ChatRepository interface {
	AuthorizedChatType(context.Context, string, string) (string, error)
	ResolveMessageRecipients(context.Context, string) ([]domain.RecipientDevice, error)
	EnqueueMessageEvent(context.Context, string, domain.SendMessage, []domain.RecipientDevice) (domain.MessageAck, error)
	MarkDelivered(context.Context, string, string, int64) error
}
