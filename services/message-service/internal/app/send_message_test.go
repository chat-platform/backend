package app

import (
	"context"
	"io"
	"testing"

	"github.com/chat-platform/backend/services/message-service/internal/domain"
	"github.com/chat-platform/backend/shared/pkg/logging"
)

type chatRepositoryStub struct {
	chatType      string
	recipients    []domain.RecipientDevice
	enqueuedType  string
	enqueued      []domain.RecipientDevice
	deliveredType string
}

func (r *chatRepositoryStub) AuthorizedChatType(context.Context, string, string) (string, error) {
	return r.chatType, nil
}
func (r *chatRepositoryStub) ResolveMessageRecipients(context.Context, string) ([]domain.RecipientDevice, error) {
	return r.recipients, nil
}
func (r *chatRepositoryStub) EnqueueMessageEvent(_ context.Context, chatType string, _ domain.SendMessage, recipients []domain.RecipientDevice) (domain.MessageAck, error) {
	r.enqueuedType = chatType
	r.enqueued = recipients
	return domain.MessageAck{ClientMsgID: "client", TimestampMS: 1}, nil
}
func (r *chatRepositoryStub) MarkDelivered(_ context.Context, _, outboxType string, _ int64) error {
	r.deliveredType = outboxType
	return nil
}

func TestGroupSendAllowsNoEchoRecipientsAndMarksGroupDelivery(t *testing.T) {
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	repo := &chatRepositoryStub{
		chatType:   "GROUP",
		recipients: []domain.RecipientDevice{{UserID: "sender", DeviceID: "origin"}},
	}
	operations := NewChatOperations(repo, logger)
	_, err = operations.SendMessage(context.Background(), domain.SendMessage{SenderID: "sender", DeviceID: "origin", ChatID: "group"})
	if err != nil {
		t.Fatalf("single-device group send failed: %v", err)
	}
	if repo.enqueuedType != "GROUP" || len(repo.enqueued) != 0 {
		t.Fatalf("enqueue type=%q recipients=%+v, want GROUP and no echo rows", repo.enqueuedType, repo.enqueued)
	}
	if err := operations.MarkDelivered(context.Background(), "device", "group", 7); err != nil || repo.deliveredType != "group" {
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
}
