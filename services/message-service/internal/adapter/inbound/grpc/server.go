package grpcserver

import (
	"context"
	"errors"

	"github.com/bufbuild/protovalidate-go"
	"github.com/chat-platform/backend/services/message-service/internal/domain"
	messagev1 "github.com/chat-platform/backend/shared/gen/message/v1"
	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ChatOperations interface {
	SendMessage(context.Context, domain.SendMessage) (domain.MessageAck, error)
	MarkDelivered(context.Context, string, string, int64) error
}

type Server struct {
	messagev1.UnimplementedMessageServiceServer
	operations ChatOperations
	validator  protovalidate.Validator
	logger     logging.Logger
}

func Register(server *grpc.Server, operations ChatOperations, validator protovalidate.Validator, logger logging.Logger) {
	messagev1.RegisterMessageServiceServer(server, &Server{operations: operations, validator: validator, logger: logger})
}

func (s *Server) SendMessage(ctx context.Context, request *messagev1.SendMessageRequest) (*messagev1.SendMessageAck, error) {
	if request == nil {
		s.logger.Warn(ctx, "message send request is missing")
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := s.validator.Validate(request); err != nil {
		s.logger.Warn(ctx, "message send request failed validation", "error", err,
			"sender_id", request.GetSenderId(), "chat_id", request.GetChatId(),
			"client_msg_id", request.GetClientMsgId())
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	clientMsgID, err := uuid.Parse(request.GetClientMsgId()) // validated as UUIDv7 above
	if err != nil {
		s.logger.Warn(ctx, "validated client message ID could not be parsed", "error", err,
			"client_msg_id", request.GetClientMsgId(), "chat_id", request.GetChatId(),
			"sender_id", request.GetSenderId())
		return nil, status.Error(codes.InvalidArgument, "client_msg_id must be a valid UUID")
	}
	ack, err := s.operations.SendMessage(ctx, domain.SendMessage{
		SenderID: request.GetSenderId(), DeviceID: request.GetDeviceId(),
		ChatID: request.GetChatId(), ClientMsgID: clientMsgID.String(), Content: request.GetContent(),
	})
	if err != nil {
		if errors.Is(err, domain.ErrInvalidMessage) {
			s.logger.Warn(ctx, "message send rejected as invalid", "chat_id", request.GetChatId(),
				"sender_id", request.GetSenderId(), "client_msg_id", request.GetClientMsgId())
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if errors.Is(err, domain.ErrChatAccessDenied) {
			s.logger.Warn(ctx, "message send rejected because sender lacks chat access",
				"chat_id", request.GetChatId(), "sender_id", request.GetSenderId())
			return nil, status.Error(codes.PermissionDenied, err.Error())
		}
		if errors.Is(err, domain.ErrNoRecipients) {
			s.logger.Warn(ctx, "message send rejected because chat has no recipient devices",
				"chat_id", request.GetChatId(), "sender_id", request.GetSenderId())
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		s.logger.Error(ctx, "message send failed",
			"error", err,
			"chat_id", request.GetChatId(),
			"sender_id", request.GetSenderId(),
			"client_msg_id", request.GetClientMsgId(),
		)
		return nil, status.Error(codes.Internal, "could not persist message")
	}
	s.logger.Info(ctx, "message durably accepted",
		"chat_id", request.GetChatId(), "sender_id", request.GetSenderId(),
		"device_id", request.GetDeviceId(), "client_msg_id", ack.ClientMsgID,
		"ts_unix_milli", ack.TimestampMS)
	return &messagev1.SendMessageAck{ClientMsgId: ack.ClientMsgID, TsUnixMilli: ack.TimestampMS}, nil
}

func (s *Server) MarkDelivered(ctx context.Context, request *messagev1.MarkDeliveredRequest) (*messagev1.MarkDeliveredResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := s.validator.Validate(request); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.operations.MarkDelivered(ctx, request.GetDeviceId(), request.GetOutboxType(), request.GetOutboxId()); err != nil {
		if errors.Is(err, domain.ErrInvalidMessage) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		s.logger.Error(ctx, "could not mark outbox event delivered", "device_id", request.GetDeviceId(), "outbox_id", request.GetOutboxId(), "error", err)
		return nil, status.Error(codes.Internal, "could not mark event delivered")
	}
	return &messagev1.MarkDeliveredResponse{}, nil
}
