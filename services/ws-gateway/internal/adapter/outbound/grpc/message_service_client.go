package grpcadapter

import (
	"context"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	messagev1 "github.com/chat-platform/backend/shared/gen/message/v1"
	"github.com/chat-platform/backend/shared/pkg/logging"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type messageServiceClient struct {
	client messagev1.MessageServiceClient
	logger logging.Logger
}

func NewMessageServiceClient(client messagev1.MessageServiceClient, logger logging.Logger) *messageServiceClient {
	return &messageServiceClient{client: client, logger: logger}
}

func (s *messageServiceClient) SendMessage(ctx context.Context, command domain.SendMessage) (domain.MessageAck, error) {
	response, err := s.client.SendMessage(ctx, &messagev1.SendMessageRequest{
		SenderId: command.SenderID, DeviceId: command.DeviceID, ChatId: command.ChatID,
		ClientMsgId: command.ClientMsgID, Content: command.Content,
	})
	if err != nil {
		code := status.Code(err)
		if code == codes.Unavailable || code == codes.DeadlineExceeded {
			s.logger.Error(ctx, "message service RPC could not be completed", "error", err,
				"sender_id", command.SenderID, "device_id", command.DeviceID,
				"chat_id", command.ChatID, "client_msg_id", command.ClientMsgID)
		}
		return domain.MessageAck{}, err
	}
	return domain.MessageAck{ClientMsgID: response.GetClientMsgId(), TimestampMS: response.GetTsUnixMilli()}, nil
}
