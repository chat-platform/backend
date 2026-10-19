package app

import (
	"context"

	"github.com/chat-platform/backend/services/message-service/internal/domain"
	"github.com/chat-platform/backend/services/message-service/internal/port"
	"github.com/chat-platform/backend/shared/pkg/logging"
)

type ChatOperations struct {
	chatRepository port.ChatRepository
	logger         logging.Logger
}

func NewChatOperations(chatRepository port.ChatRepository, logger logging.Logger) *ChatOperations {
	return &ChatOperations{chatRepository: chatRepository, logger: logger}
}

func (s *ChatOperations) SendMessage(ctx context.Context, command domain.SendMessage) (domain.MessageAck, error) {
	allowed, err := s.chatRepository.IsAuthorizedToSend(ctx, command.ChatID, command.SenderID)
	if err != nil {
		return domain.MessageAck{}, err
	}
	if !allowed {
		return domain.MessageAck{}, domain.ErrChatAccessDenied
	}

	allRecipients, err := s.chatRepository.ResolveMessageRecipients(ctx, command.ChatID)
	if err != nil {
		return domain.MessageAck{}, err
	}
	recipients := make([]domain.RecipientDevice, 0, len(allRecipients))
	for _, recipient := range allRecipients {
		if recipient.DeviceID != command.DeviceID {
			recipients = append(recipients, recipient)
		}
	}
	if len(recipients) == 0 {
		s.logger.Warn(ctx, "no recipient devices remain after excluding sender device",
			"chat_id", command.ChatID,
			"resolved_devices", len(allRecipients),
		)
		return domain.MessageAck{}, domain.ErrNoRecipients
	}
	return s.chatRepository.EnqueueMessageEvent(ctx, command, recipients)
}

func (s *ChatOperations) MarkDelivered(ctx context.Context, deviceID string, outboxID int64) error {
	if deviceID == "" || outboxID <= 0 {
		return domain.ErrInvalidMessage
	}
	return s.chatRepository.MarkDelivered(ctx, deviceID, outboxID)
}
