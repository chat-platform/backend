package app

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/chat-platform/backend/services/message-service/internal/domain"
	"github.com/chat-platform/backend/shared/pkg/logging"
)

type chatRepositoryStub struct {
	chatType       string
	recipients     []domain.RecipientDevice
	recipientsErr  error
	resolvedCalls  int
	directEnqueued bool
	groupEnqueued  bool
	enqueued       []domain.RecipientDevice
	deliveredType  string
}

func (r *chatRepositoryStub) AuthorizedChatType(context.Context, string, string) (string, error) {
	return r.chatType, nil
}
func (r *chatRepositoryStub) ResolveMessageRecipients(context.Context, string) ([]domain.RecipientDevice, error) {
	r.resolvedCalls++
	return r.recipients, r.recipientsErr
}
func (r *chatRepositoryStub) EnqueueDirectMessageEvent(_ context.Context, _ domain.SendMessage, recipients []domain.RecipientDevice) (domain.MessageAck, error) {
	r.directEnqueued = true
	r.enqueued = recipients
	return domain.MessageAck{ClientMsgID: "client", TimestampMS: 1}, nil
}
func (r *chatRepositoryStub) EnqueueGroupMessageEvent(_ context.Context, _ domain.SendMessage) (domain.MessageAck, error) {
	r.groupEnqueued = true
	return domain.MessageAck{ClientMsgID: "client", TimestampMS: 1}, nil
}
func (r *chatRepositoryStub) MarkDelivered(_ context.Context, receipt domain.Receipt) (domain.DeliveryAck, error) {
	r.deliveredType = receipt.OutboxType
	return domain.DeliveryAck{}, nil
}
func (r *chatRepositoryStub) MarkRead(context.Context, domain.Receipt) error   { return nil }
func (r *chatRepositoryStub) MarkPlayed(context.Context, domain.Receipt) error { return nil }

func TestGroupSendAllowsNoEchoRecipientsAndMarksGroupDelivery(t *testing.T) {
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	repo := &chatRepositoryStub{
		chatType: "GROUP",
	}
	operations := NewChatOperations(repo, logger)
	_, err = operations.SendMessage(context.Background(), domain.SendMessage{SenderID: "sender", DeviceID: "origin", ChatID: "group"})
	if err != nil {
		t.Fatalf("single-device group send failed: %v", err)
	}
	if !repo.groupEnqueued || repo.directEnqueued || repo.resolvedCalls != 0 {
		t.Fatalf("group dispatch: group=%v direct=%v recipient queries=%d", repo.groupEnqueued, repo.directEnqueued, repo.resolvedCalls)
	}
	if _, err := operations.MarkDelivered(context.Background(), domain.Receipt{DeviceID: "device", ReceiptSenderID: "reader", ChatID: "chat", MessageSenderID: "sender", ClientMsgID: "client", OutboxType: "group", OutboxID: 7}); err != nil || repo.deliveredType != "group" {
		t.Fatalf("group delivery mark type=%q error=%v", repo.deliveredType, err)
	}
}

func TestDirectSendWithoutRecipientDevicesIsAcknowledgedAndDiscarded(t *testing.T) {
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name          string
		recipients    []domain.RecipientDevice
		recipientsErr error
	}{
		{name: "no registered devices", recipientsErr: domain.ErrNoRecipients},
		{name: "sender device only", recipients: []domain.RecipientDevice{{UserID: "sender", DeviceID: "origin"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &chatRepositoryStub{chatType: "DIRECT", recipients: tc.recipients, recipientsErr: tc.recipientsErr}
			command := domain.SendMessage{SenderID: "sender", DeviceID: "origin", ChatID: "chat", ClientMsgID: "client"}
			ack, err := NewChatOperations(repo, logger).SendMessage(context.Background(), command)
			if err != nil {
				t.Fatalf("direct send returned error: %v", err)
			}
			if ack.ClientMsgID != command.ClientMsgID || ack.TimestampMS <= 0 {
				t.Fatalf("ack=%+v, want client ID %q and a server timestamp", ack, command.ClientMsgID)
			}
			if repo.directEnqueued || repo.groupEnqueued {
				t.Fatalf("message with no recipient was enqueued: direct=%v group=%v", repo.directEnqueued, repo.groupEnqueued)
			}
		})
	}
}

func TestDirectSendPreservesRecipientRepositoryErrors(t *testing.T) {
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("repository unavailable")
	repo := &chatRepositoryStub{chatType: "DIRECT", recipientsErr: wantErr}
	_, err = NewChatOperations(repo, logger).SendMessage(context.Background(), domain.SendMessage{SenderID: "sender", DeviceID: "origin", ChatID: "chat"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("send error=%v, want wrapped repository error %v", err, wantErr)
	}
}

func TestDirectSendRetryCanDeliverWhenRecipientDeviceAppears(t *testing.T) {
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	repo := &chatRepositoryStub{chatType: "DIRECT", recipientsErr: domain.ErrNoRecipients}
	operations := NewChatOperations(repo, logger)
	command := domain.SendMessage{SenderID: "sender", DeviceID: "origin", ChatID: "chat", ClientMsgID: "client"}
	if _, err := operations.SendMessage(context.Background(), command); err != nil {
		t.Fatalf("first send returned error: %v", err)
	}
	repo.recipientsErr = nil
	repo.recipients = []domain.RecipientDevice{{UserID: "recipient", DeviceID: "new-device"}}
	if _, err := operations.SendMessage(context.Background(), command); err != nil {
		t.Fatalf("retry returned error: %v", err)
	}
	if !repo.directEnqueued || len(repo.enqueued) != 1 || repo.enqueued[0].DeviceID != "new-device" {
		t.Fatalf("retry did not enqueue for newly available recipient: %+v", repo.enqueued)
	}
}
