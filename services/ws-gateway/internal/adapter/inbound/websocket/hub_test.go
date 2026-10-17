package websocket

import (
	"context"
	"errors"
	"testing"

	"github.com/chat-platform/backend/shared/pkg/wsframe"
)

type captureSocket struct {
	value    any
	writeErr error
	closed   bool
	onWrite  func()
}

func (s *captureSocket) WriteJSON(v any) error {
	s.value = v
	if s.onWrite != nil {
		s.onWrite()
	}
	return s.writeErr
}
func (s *captureSocket) Close() error { s.closed = true; return nil }
func TestHubDeliversToRegisteredConnection(t *testing.T) {
	h := NewHub()
	socket := &captureSocket{}
	h.Register("conn", socket)
	want := wsframe.Envelope{Type: wsframe.TypeMessageRecv, ID: "9"}
	if err := h.Deliver(context.Background(), "conn", want); err != nil {
		t.Fatal(err)
	}
	got, ok := socket.value.(wsframe.Envelope)
	if !ok || got.ID != want.ID {
		t.Fatalf("delivered=%#v", socket.value)
	}
}
func TestHubReportsMissingConnection(t *testing.T) {
	if err := NewHub().Deliver(context.Background(), "gone", wsframe.Envelope{}); err != ErrConnectionGone {
		t.Fatalf("err=%v, want ErrConnectionGone", err)
	}
}
func TestHubRemoveDoesNotRemoveReplacement(t *testing.T) {
	h := NewHub()
	oldSocket, newSocket := &captureSocket{}, &captureSocket{}
	h.Register("conn", oldSocket)
	h.Register("conn", newSocket)
	h.Remove("conn", oldSocket)
	if err := h.Deliver(context.Background(), "conn", wsframe.Envelope{ID: "still-here"}); err != nil {
		t.Fatal(err)
	}
	if newSocket.value == nil {
		t.Fatal("replacement socket was removed")
	}
}

func TestHubWriteFailureRemovesAndClosesFailedSocket(t *testing.T) {
	h := NewHub()
	writeErr := errors.New("broken websocket")
	socket := &captureSocket{writeErr: writeErr}
	h.Register("conn", socket)

	err := h.Deliver(context.Background(), "conn", wsframe.Envelope{})
	if !errors.Is(err, ErrConnectionGone) {
		t.Fatalf("err=%v, want ErrConnectionGone", err)
	}
	if !errors.Is(err, writeErr) {
		t.Fatalf("err=%v, want original write error preserved", err)
	}
	if !socket.closed {
		t.Fatal("failed socket was not closed")
	}
	if err := h.Deliver(context.Background(), "conn", wsframe.Envelope{}); err != ErrConnectionGone {
		t.Fatalf("delivery after eviction err=%v, want ErrConnectionGone", err)
	}
}

func TestHubWriteFailureCannotRemoveOrCloseReplacement(t *testing.T) {
	h := NewHub()
	oldSocket, replacement := &captureSocket{writeErr: errors.New("broken old websocket")}, &captureSocket{}
	oldSocket.onWrite = func() { h.Register("conn", replacement) }
	h.Register("conn", oldSocket)

	if err := h.Deliver(context.Background(), "conn", wsframe.Envelope{}); !errors.Is(err, ErrConnectionGone) {
		t.Fatalf("err=%v, want ErrConnectionGone", err)
	}
	if !oldSocket.closed {
		t.Fatal("failed old socket was not closed")
	}
	if replacement.closed {
		t.Fatal("replacement socket was closed")
	}
	if err := h.Deliver(context.Background(), "conn", wsframe.Envelope{ID: "replacement"}); err != nil {
		t.Fatalf("replacement delivery failed: %v", err)
	}
	if replacement.value == nil {
		t.Fatal("replacement socket was removed from the hub")
	}
}
