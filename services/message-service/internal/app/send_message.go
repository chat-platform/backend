package app

import (
	"context"
	"errors"
	"time"

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
	chatType, err := s.chatRepository.AuthorizedChatType(ctx, command.ChatID, command.SenderID)
	if err != nil {
		return domain.MessageAck{}, err
	}
	if chatType == "" {
		return domain.MessageAck{}, domain.ErrChatAccessDenied
	}

	switch chatType {
	case "DIRECT":
		allRecipients, err := s.chatRepository.ResolveMessageRecipients(ctx, command.ChatID)
		if err != nil && !errors.Is(err, domain.ErrNoRecipients) {
			return domain.MessageAck{}, err
		}
		if errors.Is(err, domain.ErrNoRecipients) {
			allRecipients = nil
		}
		recipients := make([]domain.RecipientDevice, 0, len(allRecipients))
		for _, recipient := range allRecipients {
			if recipient.DeviceID != command.DeviceID {
				recipients = append(recipients, recipient)
			}
		}
		if len(recipients) == 0 {
			s.logger.Info(ctx, "direct message accepted without recipient devices; discarding",
				"chat_id", command.ChatID,
				"resolved_devices", len(allRecipients),
			)
			return domain.MessageAck{ClientMsgID: command.ClientMsgID, TimestampMS: time.Now().UnixMilli()}, nil
		}
		return s.chatRepository.EnqueueDirectMessageEvent(ctx, command, recipients)
	case "GROUP":
		return s.chatRepository.EnqueueGroupMessageEvent(ctx, command)
	default:
		return domain.MessageAck{}, domain.ErrChatAccessDenied
	}
}

func (s *ChatOperations) MarkDelivered(ctx context.Context, receipt domain.Receipt) (domain.DeliveryAck, error) {
	if receipt.DeviceID == "" || receipt.ReceiptSenderID == "" || (receipt.OutboxType != "direct" && receipt.OutboxType != "group") || receipt.OutboxID <= 0 || receipt.ChatID == "" || receipt.MessageSenderID == "" || receipt.ClientMsgID == "" {
		return domain.DeliveryAck{}, domain.ErrInvalidMessage
	}
	return s.chatRepository.MarkDelivered(ctx, receipt)
}

func (s *ChatOperations) MarkRead(ctx context.Context, receipt domain.Receipt) error {
	if receipt.DeviceID == "" || receipt.ReceiptSenderID == "" || receipt.ChatID == "" || receipt.MessageSenderID == "" || receipt.ClientMsgID == "" {
		return domain.ErrInvalidMessage
	}
	return s.chatRepository.MarkRead(ctx, receipt)
}

func (s *ChatOperations) MarkPlayed(ctx context.Context, receipt domain.Receipt) error {
	if receipt.DeviceID == "" || receipt.ReceiptSenderID == "" || receipt.ChatID == "" || receipt.MessageSenderID == "" || receipt.ClientMsgID == "" {
		return domain.ErrInvalidMessage
	}
	return s.chatRepository.MarkPlayed(ctx, receipt)
}
