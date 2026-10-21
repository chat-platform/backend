package app

import (
	"context"
	"io"
	"testing"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	"github.com/chat-platform/backend/shared/pkg/logging"
)

type messageSenderFunc func(context.Context, domain.SendMessage) (domain.MessageAck, error)

func (f messageSenderFunc) SendMessage(ctx context.Context, command domain.SendMessage) (domain.MessageAck, error) {
	return f(ctx, command)
}
func (messageSenderFunc) MarkDelivered(context.Context, string, string, int64) error { return nil }
func (messageSenderFunc) MarkRead(context.Context, string, string, int64, int64) error {
	return nil
}

type registryStub struct{}

func (registryStub) Register(context.Context, string, domain.Connection) error { return nil }
func (registryStub) Refresh(context.Context, string, domain.Connection) (bool, error) {
	return true, nil
}
func (registryStub) Unregister(context.Context, string, domain.Connection) error { return nil }

func TestSendAddsStubIdentityBeforeCallingMessageService(t *testing.T) {
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	identity := domain.Identity{UserID: "user-1", DeviceID: "device-1"}
	messageServiceCalled := false
	gateway := New("node-1", registryStub{}, messageSenderFunc(func(_ context.Context, command domain.SendMessage) (domain.MessageAck, error) {
		messageServiceCalled = true
		if command.SenderID != identity.UserID || command.DeviceID != identity.DeviceID {
			t.Fatalf("identity not copied to service request: %+v", command)
		}
		return domain.MessageAck{ClientMsgID: command.ClientMsgID, TimestampMS: 123}, nil
	}), logger)

	ack, err := gateway.Send(context.Background(), identity, domain.SendMessage{
		ChatID: "chat-1", ClientMsgID: "client-msg-1", Content: "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !messageServiceCalled || ack.ClientMsgID != "client-msg-1" || ack.TimestampMS != 123 {
		t.Fatalf("unexpected send result: called=%v ack=%+v", messageServiceCalled, ack)
	}
}
