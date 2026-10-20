package redisinbound

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	"github.com/redis/go-redis/v9"
)

type fakeRevocationSubscription struct {
	receiveErr error
	messages   chan *redis.Message
}

func (s *fakeRevocationSubscription) Receive(ctx context.Context) (interface{}, error) {
	if s.receiveErr != nil {
		return nil, s.receiveErr
	}
	if s.messages == nil {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return nil, nil
}
func (s *fakeRevocationSubscription) Channel(...redis.ChannelOption) <-chan *redis.Message {
	return s.messages
}
func (s *fakeRevocationSubscription) Close() error { return nil }

func TestRevocationConsumerRetriesFailedAndClosedSubscriptions(t *testing.T) {
	var attempts atomic.Int32
	var reports atomic.Int32
	messages := make(chan *redis.Message, 1)
	messages <- &redis.Message{Payload: `{"node_id":"node-1","conn_id":"conn-1","session_id":"revoked-session"}`}
	close(messages)
	consumer := &RevocationConsumer{
		retryMin:    time.Millisecond,
		retryMax:    2 * time.Millisecond,
		reportError: func(error) { reports.Add(1) },
		subscribe: func(context.Context) revocationSubscription {
			switch attempts.Add(1) {
			case 1:
				return &fakeRevocationSubscription{receiveErr: errors.New("redis unavailable")}
			case 2:
				return &fakeRevocationSubscription{messages: messages}
			default:
				return &fakeRevocationSubscription{}
			}
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	received := make(chan domain.Connection, 1)
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx, func(_ context.Context, connection domain.Connection) { received <- connection })
	}()

	select {
	case got := <-received:
		if got.NodeID != "node-1" || got.ConnID != "conn-1" || got.SessionID != "revoked-session" {
			t.Fatalf("revocation payload=%+v", got)
		}
	case <-ctx.Done():
		t.Fatal("revocation event was not received")
	}
	deadline := time.After(time.Second)
	for attempts.Load() < 3 {
		select {
		case <-deadline:
			t.Fatal("consumer did not retry after the subscription channel closed")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error on cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("consumer did not stop after cancellation")
	}
	if reports.Load() < 2 {
		t.Fatalf("reported retry errors=%d, want at least 2", reports.Load())
	}
}
