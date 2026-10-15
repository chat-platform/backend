package grpcadapter

import (
	"context"

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
		return domain.MessageAck{}, err
	}
	return domain.MessageAck{ClientMsgID: response.GetClientMsgId(), TimestampMS: response.GetTsUnixMilli()}, nil
}
