package grpcadapter

import (
	"context"
	"fmt"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	messagev1 "github.com/chat-platform/backend/shared/gen/message/v1"
)

type messageServiceClient struct {
	client messagev1.MessageServiceClient
}

func NewMessageServiceClient(client messagev1.MessageServiceClient) *messageServiceClient {
	return &messageServiceClient{client: client}
}

func (s *messageServiceClient) SendMessage(ctx context.Context, command domain.SendMessage) (domain.MessageAck, error) {
	response, err := s.client.SendMessage(ctx, &messagev1.SendMessageRequest{
		SenderId: command.SenderID, DeviceId: command.DeviceID, ChatId: command.ChatID,
		ClientMsgId: command.ClientMsgID, Content: command.Content,
	})
	if err != nil {
		return domain.MessageAck{}, fmt.Errorf("message service SendMessage RPC: %w", err)
	}
	return domain.MessageAck{ClientMsgID: response.GetClientMsgId(), TimestampMS: response.GetTsUnixMilli()}, nil
}

func (s *messageServiceClient) MarkDelivered(ctx context.Context, deviceID, receiptSenderID, chatID, messageSenderID, clientMsgID, outboxType string, outboxID int64) error {
	_, err := s.client.MarkDelivered(ctx, &messagev1.MarkDeliveredRequest{DeviceId: deviceID, ReceiptSenderId: receiptSenderID, ChatId: chatID, MessageSenderId: messageSenderID, ClientMsgId: clientMsgID, OutboxType: outboxType, OutboxId: outboxID})
	if err != nil {
		return fmt.Errorf("message service MarkDelivered RPC: %w", err)
	}
	return nil
}

func (s *messageServiceClient) MarkRead(ctx context.Context, deviceID, receiptSenderID, chatID, messageSenderID, clientMsgID string) error {
	_, err := s.client.MarkRead(ctx, &messagev1.MarkReadRequest{DeviceId: deviceID, ReceiptSenderId: receiptSenderID, ChatId: chatID, MessageSenderId: messageSenderID, ClientMsgId: clientMsgID})
	if err != nil {
		return fmt.Errorf("message service MarkRead RPC: %w", err)
	}
	return nil
}
