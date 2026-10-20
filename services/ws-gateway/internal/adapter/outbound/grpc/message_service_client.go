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

func (s *messageServiceClient) MarkDelivered(ctx context.Context, deviceID string, outboxType string, outboxID int64) error {
	_, err := s.client.MarkDelivered(ctx, &messagev1.MarkDeliveredRequest{DeviceId: deviceID, OutboxType: outboxType, OutboxId: outboxID})
	if err != nil {
		if code := status.Code(err); code == codes.Unavailable || code == codes.DeadlineExceeded {
			s.logger.Error(ctx, "message service delivery receipt RPC could not be completed", "error", err, "device_id", deviceID, "outbox_id", outboxID)
		}
		return err
	}
	return nil
}

func (s *messageServiceClient) MarkRead(ctx context.Context, deviceID, readerID, chatID, senderID, clientMsgID string, readAt int64) error {
	_, err := s.client.MarkRead(ctx, &messagev1.MarkReadRequest{DeviceId: deviceID, ReaderUserId: readerID, ChatId: chatID, SenderId: senderID, ClientMsgId: clientMsgID, ReadAt: readAt})
	return err
}
