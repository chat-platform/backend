package authadapter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
)

type statusStub struct {
	active bool
	err    error
}

func (s statusStub) IsSessionActive(context.Context, string) (bool, error) { return s.active, s.err }

func TestAuthenticateValidatesJWTAndRedisSessionState(t *testing.T) {
	secret := []byte("a sufficiently long test signing secret 123456")
	now := time.Now()
	token, err := sessiontoken.Sign(sessiontoken.Claims{SessionID: "sid", UserID: "user", DeviceID: "device", IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Hour).Unix()}, secret)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := New(secret, statusStub{active: true}).Authenticate(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	if identity.SessionID != "sid" || identity.UserID != "user" || identity.DeviceID != "device" {
		t.Fatalf("identity=%+v", identity)
	}
	if _, err := New(secret, statusStub{active: false}).Authenticate(context.Background(), token); err == nil {
		t.Fatal("revoked session was accepted")
	}
	if _, err := New(secret, statusStub{active: true}).Authenticate(context.Background(), token+"x"); err == nil {
		t.Fatal("invalid JWT was accepted")
	}
	if _, err := New(secret, statusStub{err: errors.New("redis down")}).Authenticate(context.Background(), token); err == nil {
		t.Fatal("Redis failure was accepted")
	}
}

func TestAuthenticateRejectsExpiredJWT(t *testing.T) {
	secret := []byte("a sufficiently long test signing secret 123456")
	now := time.Now().Add(-2 * time.Hour)
	token, err := sessiontoken.Sign(sessiontoken.Claims{SessionID: "sid", UserID: "user", DeviceID: "device", IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Hour).Unix()}, secret)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(secret, statusStub{active: true}).Authenticate(context.Background(), token); err == nil {
		t.Fatal("expired JWT was accepted")
	}
}
