package app

import (
	"context"
	"io"
	"testing"

	"github.com/chat-platform/backend/services/message-service/internal/domain"
	"github.com/chat-platform/backend/shared/pkg/logging"
)

type chatRepositoryStub struct {
	chatType       string
	recipients     []domain.RecipientDevice
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
	return r.recipients, nil
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
func (r *chatRepositoryStub) MarkDelivered(_ context.Context, receipt domain.Receipt) error {
	r.deliveredType = receipt.OutboxType
	return nil
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
	if err := operations.MarkDelivered(context.Background(), domain.Receipt{DeviceID: "device", ReceiptSenderID: "reader", ChatID: "chat", MessageSenderID: "sender", ClientMsgID: "client", OutboxType: "group", OutboxID: 7}); err != nil || repo.deliveredType != "group" {
		t.Fatalf("group delivery mark type=%q error=%v", repo.deliveredType, err)
	}
}

func TestDirectSendStillRequiresAtLeastOneOtherDevice(t *testing.T) {
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	repo := &chatRepositoryStub{
		chatType:   "DIRECT",
		recipients: []domain.RecipientDevice{{UserID: "sender", DeviceID: "origin"}},
	}
	_, err = NewChatOperations(repo, logger).SendMessage(context.Background(), domain.SendMessage{SenderID: "sender", DeviceID: "origin"})
	if err != domain.ErrNoRecipients {
		t.Fatalf("direct send error=%v, want ErrNoRecipients", err)
	}
	if repo.directEnqueued || repo.groupEnqueued {
		t.Fatalf("empty direct recipient set enqueued: direct=%v group=%v", repo.directEnqueued, repo.groupEnqueued)
	}
}
