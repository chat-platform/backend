package grpcserver

import (
	"context"
	"errors"

	"github.com/chat-platform/backend/services/message-service/internal/app"
	"github.com/chat-platform/backend/services/message-service/internal/domain"
	messagev1 "github.com/chat-platform/backend/shared/gen/message/v1"
	"github.com/bufbuild/protovalidate-go"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	messagev1.UnimplementedMessageServiceServer
	send      *app.SendMessage
	validator protovalidate.Validator
}

func Register(server *grpc.Server, send *app.SendMessage, validator protovalidate.Validator) {
	messagev1.RegisterMessageServiceServer(server, &Server{send: send, validator: validator})
}

func (s *Server) SendMessage(ctx context.Context, request *messagev1.SendMessageRequest) (*messagev1.SendMessageAck, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := s.validator.Validate(request); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	clientMsgID, _ := uuid.Parse(request.GetClientMsgId()) // validated as UUIDv7 above
	ack, err := s.send.Execute(ctx, domain.SendMessage{
		SenderID: request.GetSenderId(), DeviceID: request.GetDeviceId(),
		ChatID: request.GetChatId(), ClientMsgID: clientMsgID.String(), Content: request.GetContent(),
	})
	if err != nil {
		if errors.Is(err, domain.ErrInvalidMessage) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, "could not persist message")
	}
	return &messagev1.SendMessageAck{ClientMsgId: ack.ClientMsgID, TsUnixMilli: ack.TimestampMS}, nil
}
