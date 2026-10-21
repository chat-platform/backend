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
		args := []any{"error", err, "grpc_code", code.String(),
			"sender_id", command.SenderID, "device_id", command.DeviceID,
			"chat_id", command.ChatID, "client_msg_id", command.ClientMsgID}
		if code == codes.InvalidArgument || code == codes.PermissionDenied || code == codes.FailedPrecondition || code == codes.Unauthenticated {
			s.logger.Warn(ctx, "message service send RPC failed", args...)
		} else {
			s.logger.Error(ctx, "message service send RPC failed", args...)
		}
		return domain.MessageAck{}, err
	}
	return domain.MessageAck{ClientMsgID: response.GetClientMsgId(), TimestampMS: response.GetTsUnixMilli()}, nil
}

func (s *messageServiceClient) MarkDelivered(ctx context.Context, deviceID string, outboxType string, outboxID int64) error {
	_, err := s.client.MarkDelivered(ctx, &messagev1.MarkDeliveredRequest{DeviceId: deviceID, OutboxType: outboxType, OutboxId: outboxID})
	if err != nil {
		code := status.Code(err)
		if code == codes.InvalidArgument || code == codes.PermissionDenied || code == codes.NotFound {
			s.logger.Warn(ctx, "message service delivery receipt RPC failed", "error", err,
				"grpc_code", code.String(), "device_id", deviceID, "outbox_type", outboxType, "outbox_id", outboxID)
		} else {
			s.logger.Error(ctx, "message service delivery receipt RPC failed", "error", err,
				"grpc_code", code.String(), "device_id", deviceID, "outbox_type", outboxType, "outbox_id", outboxID)
		}
		return err
	}
	return nil
}

func (s *messageServiceClient) MarkRead(ctx context.Context, deviceID, outboxType string, outboxID, readAt int64) error {
	_, err := s.client.MarkRead(ctx, &messagev1.MarkReadRequest{DeviceId: deviceID, OutboxType: outboxType, OutboxId: outboxID, ReadAt: readAt})
	return err
}
