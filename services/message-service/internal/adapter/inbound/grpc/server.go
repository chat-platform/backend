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
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := s.validator.Validate(request); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	clientMsgID, _ := uuid.Parse(request.GetClientMsgId()) // validated as UUIDv7 above
	ack, err := s.operations.SendMessage(ctx, domain.SendMessage{
		SenderID: request.GetSenderId(), DeviceID: request.GetDeviceId(),
		ChatID: request.GetChatId(), ClientMsgID: clientMsgID.String(), Content: request.GetContent(),
	})
	if err != nil {
		if errors.Is(err, domain.ErrInvalidMessage) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if errors.Is(err, domain.ErrChatAccessDenied) {
			return nil, status.Error(codes.PermissionDenied, err.Error())
		}
		if errors.Is(err, domain.ErrNoRecipients) {
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
	return &messagev1.SendMessageAck{ClientMsgId: ack.ClientMsgID, TsUnixMilli: ack.TimestampMS}, nil
}
