package authadapter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
)

type statusStub struct {
	status string
	err    error
	calls  int
}

func (s *statusStub) SessionStatus(context.Context, string) (string, error) {
	s.calls++
	return s.status, s.err
}

type validatorStub struct {
	claims sessiontoken.Claims
	err    error
	calls  int
	token  string
}

func (v *validatorStub) ValidateSession(_ context.Context, token string) (sessiontoken.Claims, error) {
	v.calls++
	v.token = token
	return v.claims, v.err
}

func TestAuthenticateUsesRedisFastPathAndRejectsRevoked(t *testing.T) {
	secret := []byte("a sufficiently long test signing secret 123456")
	token := signedTestToken(t, secret)
	validator := &validatorStub{}

	identity, err := New(secret, &statusStub{status: sessiontoken.StatusActive}, validator).Authenticate(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	if identity.SessionID != "sid" || identity.UserID != "user" || identity.DeviceID != "device" {
		t.Fatalf("identity=%+v", identity)
	}
	if validator.calls != 0 {
		t.Fatalf("Core API called on active cache hit %d times", validator.calls)
	}
	if _, err := New(secret, &statusStub{status: sessiontoken.StatusRevoked}, validator).Authenticate(context.Background(), token); !errors.Is(err, domain.ErrInvalidIdentity) {
		t.Fatalf("revoked cache status error=%v", err)
	}
}

func TestAuthenticateDelegatesCacheMissAndComparesClaims(t *testing.T) {
	secret := []byte("a sufficiently long test signing secret 123456")
	token := signedTestToken(t, secret)
	claims, err := sessiontoken.Verify(token, secret, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	validator := &validatorStub{claims: claims}
	identity, err := New(secret, &statusStub{status: sessiontoken.StatusMissing}, validator).Authenticate(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	if validator.calls != 1 || validator.token != token || identity.SessionID != claims.SessionID {
		t.Fatalf("fallback calls=%d token=%q identity=%+v", validator.calls, validator.token, identity)
	}

	validator.claims.UserID = "different-user"
	if _, err := New(secret, &statusStub{status: sessiontoken.StatusMissing}, validator).Authenticate(context.Background(), token); !errors.Is(err, domain.ErrInvalidIdentity) {
		t.Fatalf("mismatched Core API claims error=%v", err)
	}
}

func TestHeartbeatRevalidatesWhenActiveCacheEntryExpires(t *testing.T) {
	secret := []byte("a sufficiently long test signing secret 123456")
	token := signedTestToken(t, secret)
	status := &statusStub{status: sessiontoken.StatusActive}
	validator := &validatorStub{}
	authenticator := New(secret, status, validator)
	if active, err := authenticator.IsSessionActive(context.Background(), token); err != nil || !active {
		t.Fatalf("active cache result=(%v,%v)", active, err)
	}
	status.status = sessiontoken.StatusMissing // The capped status TTL elapsed.
	validator.err = domain.ErrInvalidIdentity  // Core API's Postgres check says revoked.
	if active, err := authenticator.IsSessionActive(context.Background(), token); active || !errors.Is(err, domain.ErrInvalidIdentity) {
		t.Fatalf("revoked session after cache expiry result=(%v,%v)", active, err)
	}
}

func TestAuthenticateFailsClosedOnRedisOrCoreAPIErrors(t *testing.T) {
	secret := []byte("a sufficiently long test signing secret 123456")
	token := signedTestToken(t, secret)
	validator := &validatorStub{claims: sessiontoken.Claims{SessionID: "sid", UserID: "user", DeviceID: "device", IssuedAt: time.Now().Unix(), ExpiresAt: time.Now().Add(time.Hour).Unix()}}
	if _, err := New(secret, &statusStub{err: errors.New("redis down")}, validator).Authenticate(context.Background(), token); err == nil {
		t.Fatal("Redis failure was accepted")
	}
	validator.err = errors.New("Core API unavailable")
	if _, err := New(secret, &statusStub{status: sessiontoken.StatusMissing}, validator).Authenticate(context.Background(), token); err == nil {
		t.Fatal("Core API failure was accepted")
	}
	if _, err := New(secret, &statusStub{status: "unknown"}, validator).Authenticate(context.Background(), token); !errors.Is(err, domain.ErrInvalidIdentity) {
		t.Fatalf("unknown cache state error=%v", err)
	}
}

func TestAuthenticateRejectsInvalidAndExpiredTokens(t *testing.T) {
	secret := []byte("a sufficiently long test signing secret 123456")
	status := &statusStub{status: sessiontoken.StatusActive}
	validator := &validatorStub{}
	if _, err := New(secret, status, validator).Authenticate(context.Background(), "not-a-token"); err == nil {
		t.Fatal("invalid token was accepted")
	} else if !errors.Is(err, domain.ErrInvalidIdentity) || !errors.Is(err, sessiontoken.ErrInvalidToken) {
		t.Fatalf("invalid token causes were not preserved: %v", err)
	}
	expired := sessiontoken.Claims{SessionID: "sid", UserID: "user", DeviceID: "device", IssuedAt: time.Now().Add(-2 * time.Hour).Unix(), ExpiresAt: time.Now().Add(-time.Hour).Unix()}
	token, err := sessiontoken.Sign(expired, secret)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(secret, status, validator).Authenticate(context.Background(), token); err == nil {
		t.Fatal("expired token was accepted")
	}
}

func signedTestToken(t *testing.T, secret []byte) string {
	t.Helper()
	now := time.Now()
	token, err := sessiontoken.Sign(sessiontoken.Claims{
		SessionID: "sid", UserID: "user", DeviceID: "device",
		IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Hour).Unix(),
	}, secret)
	if err != nil {
		t.Fatal(err)
	}
	return token
}
