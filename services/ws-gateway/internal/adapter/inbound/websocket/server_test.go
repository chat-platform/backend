package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/chat-platform/backend/shared/pkg/wsframe"
	"github.com/gorilla/websocket"
)

type fakeOperations struct {
	identity    domain.Identity
	command     domain.SendMessage
	registered  bool
	removed     bool
	registerErr error
	sendErr     error
	activeCheck func() bool
	activeCalls int
	removedCh   chan struct{}
}

func (f *fakeOperations) Register(_ context.Context, identity domain.Identity, connID string) (domain.Connection, error) {
	f.identity = identity
	f.registered = true
	if f.registerErr != nil {
		return domain.Connection{}, f.registerErr
	}
	return domain.Connection{NodeID: "test-node", ConnID: connID, SessionID: identity.SessionID}, nil
}

func (f *fakeOperations) Authenticate(_ context.Context, token string) (domain.Identity, error) {
	if token != "valid-test-token" {
		return domain.Identity{}, domain.ErrInvalidIdentity
	}
	return domain.Identity{UserID: "11111111-1111-4111-8111-111111111111", DeviceID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", SessionID: "ssssssss-ssss-4sss-8sss-ssssssssssss"}, nil
}
func (f *fakeOperations) IsSessionActive(context.Context, string) (bool, error) {
	f.activeCalls++
	if f.activeCheck != nil {
		return f.activeCheck(), nil
	}
	return true, nil
}

func (f *fakeOperations) Refresh(context.Context, string, domain.Connection) (bool, error) {
	return true, nil
}
func (f *fakeOperations) Unregister(context.Context, string, domain.Connection) error {
	f.removed = true
	if f.removedCh != nil {
		close(f.removedCh)
	}
	return nil
}
func (f *fakeOperations) Send(_ context.Context, identity domain.Identity, command domain.SendMessage) (domain.MessageAck, error) {
	f.identity = identity
	f.command = command
	if f.sendErr != nil {
		return domain.MessageAck{}, f.sendErr
	}
	return domain.MessageAck{ClientMsgID: command.ClientMsgID, TimestampMS: 1770000000123}, nil
}

func TestConnectionAuthSendAndAck(t *testing.T) {
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	operations := &fakeOperations{}
	httpServer := httptest.NewServer(NewServer(operations, logger, NewHub()))
	defer httpServer.Close()

	url := "ws" + strings.TrimPrefix(httpServer.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	identity := wsframe.Envelope{Type: wsframe.TypeAuth, Payload: json.RawMessage(`{"token":"valid-test-token"}`)}
	if err := conn.WriteJSON(identity); err != nil {
		t.Fatal(err)
	}
	var authAck wsframe.Envelope
	if err := conn.ReadJSON(&authAck); err != nil {
		t.Fatal(err)
	}
	if authAck.Type != wsframe.TypeAuthOK {
		t.Fatalf("unexpected auth response: %+v", authAck)
	}
	send := wsframe.Envelope{Type: wsframe.TypeMessageSend, ID: "request-1", Payload: json.RawMessage(`{"chatId":"cccccccc-cccc-4ccc-8ccc-cccccccccccc","clientMsgId":"0199d628-39d0-7000-8000-000000000001","content":"hello"}`)}
	if err := conn.WriteJSON(send); err != nil {
		t.Fatal(err)
	}
	var ack wsframe.Envelope
	if err := conn.ReadJSON(&ack); err != nil {
		t.Fatal(err)
	}
	if ack.Type != wsframe.TypeMessageAck || ack.ID != send.ID || ack.TS != 1770000000123 {
		t.Fatalf("unexpected ack envelope: %+v", ack)
	}
	var payload wsframe.MessageAckPayload
	if err := json.Unmarshal(ack.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.ClientMsgID != "0199d628-39d0-7000-8000-000000000001" || payload.TS != ack.TS {
		t.Fatalf("unexpected ack payload: %+v", payload)
	}
	if operations.identity.UserID != "11111111-1111-4111-8111-111111111111" || operations.identity.DeviceID != "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" || operations.identity.SessionID != "ssssssss-ssss-4sss-8sss-ssssssssssss" {
		t.Fatalf("identity was not forwarded: %+v", operations.identity)
	}
	if !operations.registered {
		t.Fatal("identity did not register connection")
	}
	if operations.command.ChatID != "cccccccc-cccc-4ccc-8ccc-cccccccccccc" || operations.command.Content != "hello" {
		t.Fatalf("unexpected command passed to transport operations: %+v", operations.command)
	}
}

func TestInvalidTokenIsRejected(t *testing.T) {
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	operations := &fakeOperations{}
	httpServer := httptest.NewServer(NewServer(operations, logger, NewHub()))
	defer httpServer.Close()

	url := "ws" + strings.TrimPrefix(httpServer.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	invalid := wsframe.Envelope{Type: wsframe.TypeAuth, Payload: json.RawMessage(`{"token":"invalid-token"}`)}
	if err := conn.WriteJSON(invalid); err != nil {
		t.Fatal(err)
	}
	_, _, err = conn.ReadMessage()
	if err == nil {
		t.Fatal("expected connection to close for invalid identity")
	}
	if operations.registered {
		t.Fatal("invalid identity was registered")
	}
}

func TestMalformedFrameAndSendFailureReturnErrorFrames(t *testing.T) {
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	operations := &fakeOperations{sendErr: errors.New("message service unavailable")}
	httpServer := httptest.NewServer(NewServer(operations, logger, NewHub()))
	defer httpServer.Close()

	url := "ws" + strings.TrimPrefix(httpServer.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	identity := wsframe.Envelope{Type: wsframe.TypeAuth, Payload: json.RawMessage(`{"token":"valid-test-token"}`)}
	if err := conn.WriteJSON(identity); err != nil {
		t.Fatal(err)
	}
	var authAck wsframe.Envelope
	if err := conn.ReadJSON(&authAck); err != nil || authAck.Type != wsframe.TypeAuthOK {
		t.Fatalf("auth acknowledgement=(%+v,%v)", authAck, err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, []byte("{")); err != nil {
		t.Fatal(err)
	}
	var malformed wsframe.Envelope
	if err := conn.ReadJSON(&malformed); err != nil {
		t.Fatal(err)
	}
	if malformed.Type != "error" || malformed.ID != "" {
		t.Fatalf("unexpected malformed-frame response: %+v", malformed)
	}

	send := wsframe.Envelope{Type: wsframe.TypeMessageSend, ID: "request-2", Payload: json.RawMessage(`{"chatId":"chat-1","clientMsgId":"0199d628-39d0-7000-8000-000000000001","content":"hello"}`)}
	if err := conn.WriteJSON(send); err != nil {
		t.Fatal(err)
	}
	var failed wsframe.Envelope
	if err := conn.ReadJSON(&failed); err != nil {
		t.Fatal(err)
	}
	if failed.Type != "error" || failed.ID != send.ID {
		t.Fatalf("unexpected send failure response: %+v", failed)
	}
}

func TestRegistryFailureClosesConnection(t *testing.T) {
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	operations := &fakeOperations{registerErr: errors.New("redis unavailable")}
	httpServer := httptest.NewServer(NewServer(operations, logger, NewHub()))
	defer httpServer.Close()

	url := "ws" + strings.TrimPrefix(httpServer.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	identity := wsframe.Envelope{Type: wsframe.TypeAuth, Payload: json.RawMessage(`{"token":"valid-test-token"}`)}
	if err := conn.WriteJSON(identity); err != nil {
		t.Fatal(err)
	}
	_, _, err = conn.ReadMessage()
	if err == nil {
		t.Fatal("expected registry failure to close connection")
	}
}

func TestHeartbeatRevalidationClosesAndUnregistersRevokedSession(t *testing.T) {
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	activeChecks := 0
	operations := &fakeOperations{
		removedCh: make(chan struct{}),
		activeCheck: func() bool {
			activeChecks++
			return activeChecks == 1 // Initial registration passes; later heartbeat sees revocation.
		},
	}
	server := NewServer(operations, logger, NewHub())
	server.heartbeatInterval = 10 * time.Millisecond
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()

	url := "ws" + strings.TrimPrefix(httpServer.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.WriteJSON(wsframe.Envelope{Type: wsframe.TypeAuth, Payload: json.RawMessage(`{"token":"valid-test-token"}`)}); err != nil {
		t.Fatal(err)
	}
	var authAck wsframe.Envelope
	if err := conn.ReadJSON(&authAck); err != nil {
		t.Fatalf("read auth ack: %v", err)
	}
	if authAck.Type != wsframe.TypeAuthOK {
		t.Fatalf("auth ack=%+v", authAck)
	}

	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("websocket remained open after heartbeat found revoked session")
	}
	select {
	case <-operations.removedCh:
	case <-time.After(time.Second):
		t.Fatal("revoked websocket registry entry was not unregistered")
	}
	if activeChecks < 2 {
		t.Fatalf("session active checks=%d, want heartbeat revalidation", activeChecks)
	}
}
