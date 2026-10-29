package grpcserver

import (
	"context"
	"errors"
	"time"

	"github.com/bufbuild/protovalidate-go"
	"github.com/chat-platform/backend/services/message-service/internal/domain"
	servicemetrics "github.com/chat-platform/backend/services/message-service/internal/metrics"
	messagev1 "github.com/chat-platform/backend/shared/gen/message/v1"
	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ChatOperations interface {
	SendMessage(context.Context, domain.SendMessage) (domain.MessageAck, error)
	MarkDelivered(context.Context, domain.Receipt) (domain.DeliveryAck, error)
	MarkRead(context.Context, domain.Receipt) error
	MarkPlayed(context.Context, domain.Receipt) error
}

type Server struct {
	messagev1.UnimplementedMessageServiceServer
	operations ChatOperations
	validator  protovalidate.Validator
	logger     logging.Logger
	metrics    *servicemetrics.Metrics
}

func Register(server *grpc.Server, operations ChatOperations, validator protovalidate.Validator, logger logging.Logger, metrics *servicemetrics.Metrics) {
	messagev1.RegisterMessageServiceServer(server, &Server{operations: operations, validator: validator, logger: logger, metrics: metrics})
}

func (s *Server) SendMessage(ctx context.Context, request *messagev1.SendMessageRequest) (*messagev1.SendMessageAck, error) {
	started := time.Now()
	result := servicemetrics.SendError
	defer func() { s.metrics.ObserveSend(result, time.Since(started)) }()
	if request == nil {
		result = servicemetrics.SendRejected
		s.logger.Warn(ctx, "message send request is missing")
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := s.validator.Validate(request); err != nil {
		result = servicemetrics.SendRejected
		s.logger.Warn(ctx, "message send request failed validation", "error", err,
			"sender_id", request.GetSenderId(), "chat_id", request.GetChatId(),
			"client_msg_id", request.GetClientMsgId())
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	clientMsgID, err := uuid.Parse(request.GetClientMsgId()) // validated as UUIDv7 above
	if err != nil {
		result = servicemetrics.SendRejected
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
			result = servicemetrics.SendRejected
			s.logger.Warn(ctx, "message send rejected as invalid", "chat_id", request.GetChatId(),
				"sender_id", request.GetSenderId(), "client_msg_id", request.GetClientMsgId())
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if errors.Is(err, domain.ErrChatAccessDenied) {
			result = servicemetrics.SendRejected
			s.logger.Warn(ctx, "message send rejected because sender lacks chat access",
				"chat_id", request.GetChatId(), "sender_id", request.GetSenderId())
			return nil, status.Error(codes.PermissionDenied, err.Error())
		}
		s.logger.Error(ctx, "message send failed",
			"error", err,
			"chat_id", request.GetChatId(),
			"sender_id", request.GetSenderId(),
			"client_msg_id", request.GetClientMsgId(),
		)
		return nil, status.Error(codes.Internal, "could not persist message")
	}
	result = servicemetrics.SendAccepted
	s.logger.Info(ctx, "message send accepted",
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
	ack, err := s.operations.MarkDelivered(ctx, domain.Receipt{DeviceID: request.GetDeviceId(), ReceiptSenderID: request.GetReceiptSenderId(), ChatID: request.GetChatId(), MessageSenderID: request.GetMessageSenderId(), ClientMsgID: request.GetClientMsgId(), OutboxType: request.GetOutboxType(), OutboxID: request.GetOutboxId()})
	if err != nil {
		if errors.Is(err, domain.ErrInvalidMessage) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		s.logger.Error(ctx, "could not mark outbox event delivered", "device_id", request.GetDeviceId(), "outbox_id", request.GetOutboxId(), "error", err)
		return nil, status.Error(codes.Internal, "could not mark event delivered")
	}
	if ack.FirstTransition && ack.EventType == "NEW_MESSAGE" && ack.RoutedTo == "WS" {
		s.metrics.ObserveDeliveryAck(ack.OutboxType, ack.Latency)
	}
	return &messagev1.MarkDeliveredResponse{}, nil
}

func (s *Server) MarkRead(ctx context.Context, request *messagev1.MarkReadRequest) (*messagev1.MarkReadResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := s.validator.Validate(request); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	err := s.operations.MarkRead(ctx, domain.Receipt{DeviceID: request.GetDeviceId(), ReceiptSenderID: request.GetReceiptSenderId(), ChatID: request.GetChatId(), MessageSenderID: request.GetMessageSenderId(), ClientMsgID: request.GetClientMsgId()})
	if err != nil {
		if errors.Is(err, domain.ErrInvalidMessage) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		s.logger.Error(ctx, "could not record read receipt", "error", err, "device_id", request.GetDeviceId(), "chat_id", request.GetChatId())
		return nil, status.Error(codes.Internal, "could not record read receipt")
	}
	return &messagev1.MarkReadResponse{}, nil
}

func (s *Server) MarkPlayed(ctx context.Context, request *messagev1.MarkPlayedRequest) (*messagev1.MarkPlayedResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := s.validator.Validate(request); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	err := s.operations.MarkPlayed(ctx, domain.Receipt{DeviceID: request.GetDeviceId(), ReceiptSenderID: request.GetReceiptSenderId(), ChatID: request.GetChatId(), MessageSenderID: request.GetMessageSenderId(), ClientMsgID: request.GetClientMsgId()})
	if err != nil {
		if errors.Is(err, domain.ErrInvalidMessage) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		s.logger.Error(ctx, "could not record played receipt", "error", err, "device_id", request.GetDeviceId(), "chat_id", request.GetChatId())
		return nil, status.Error(codes.Internal, "could not record played receipt")
	}
	return &messagev1.MarkPlayedResponse{}, nil
}
