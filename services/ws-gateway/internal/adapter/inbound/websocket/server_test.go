package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

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
}

func (f *fakeOperations) Register(_ context.Context, identity domain.Identity, connID string) (domain.Connection, error) {
	f.identity = identity
	f.registered = true
	if f.registerErr != nil {
		return domain.Connection{}, f.registerErr
	}
	return domain.Connection{NodeID: "test-node", ConnID: connID}, nil
}

func (f *fakeOperations) Refresh(context.Context, string, domain.Connection) (bool, error) {
	return true, nil
}
func (f *fakeOperations) Unregister(context.Context, string, domain.Connection) error {
	f.removed = true
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

func TestConnectionIdentitySendAndAck(t *testing.T) {
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	operations := &fakeOperations{}
	httpServer := httptest.NewServer(NewServer(operations, logger))
	defer httpServer.Close()

	url := "ws" + strings.TrimPrefix(httpServer.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	identity := wsframe.Envelope{Type: wsframe.TypeIdentity, Payload: json.RawMessage(`{"userId":"11111111-1111-4111-8111-111111111111","deviceId":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}`)}
	if err := conn.WriteJSON(identity); err != nil {
		t.Fatal(err)
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
	if operations.identity.UserID != "11111111-1111-4111-8111-111111111111" || operations.identity.DeviceID != "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" {
		t.Fatalf("identity was not forwarded: %+v", operations.identity)
	}
	if !operations.registered {
		t.Fatal("identity did not register connection")
	}
	if operations.command.ChatID != "cccccccc-cccc-4ccc-8ccc-cccccccccccc" || operations.command.Content != "hello" {
		t.Fatalf("unexpected command passed to transport operations: %+v", operations.command)
	}
}

func TestInvalidIdentityIsRejected(t *testing.T) {
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	operations := &fakeOperations{}
	httpServer := httptest.NewServer(NewServer(operations, logger))
	defer httpServer.Close()

	url := "ws" + strings.TrimPrefix(httpServer.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	invalid := wsframe.Envelope{Type: wsframe.TypeIdentity, Payload: json.RawMessage(`{"userId":"not-a-uuid","deviceId":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}`)}
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
	httpServer := httptest.NewServer(NewServer(operations, logger))
	defer httpServer.Close()

	url := "ws" + strings.TrimPrefix(httpServer.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	identity := wsframe.Envelope{Type: wsframe.TypeIdentity, Payload: json.RawMessage(`{"userId":"11111111-1111-4111-8111-111111111111","deviceId":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}`)}
	if err := conn.WriteJSON(identity); err != nil {
		t.Fatal(err)
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
	httpServer := httptest.NewServer(NewServer(operations, logger))
	defer httpServer.Close()

	url := "ws" + strings.TrimPrefix(httpServer.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	identity := wsframe.Envelope{Type: wsframe.TypeIdentity, Payload: json.RawMessage(`{"userId":"11111111-1111-4111-8111-111111111111","deviceId":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}`)}
	if err := conn.WriteJSON(identity); err != nil {
		t.Fatal(err)
	}
	_, _, err = conn.ReadMessage()
	if err == nil {
		t.Fatal("expected registry failure to close connection")
	}
}
