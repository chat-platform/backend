package httpadapter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/chat-platform/backend/services/core-api/internal/app"
	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
)

type sessionRepoStub struct {
	active    bool
	revoked   bool
	claims    sessiontoken.Claims
	revokeErr error
}

func (r *sessionRepoStub) CompleteOTP(context.Context, string, string, string, string, string, time.Time) (app.User, app.Device, app.Session, error) {
	return app.User{}, app.Device{}, app.Session{}, nil
}
func (r *sessionRepoStub) GetProfile(context.Context, string) (app.User, error) {
	return app.User{}, nil
}
func (r *sessionRepoStub) SetDisplayName(context.Context, string, string) error { return nil }
func (r *sessionRepoStub) GetOrCreateDirectChat(context.Context, string, string) (string, error) {
	return "", nil
}
func (r *sessionRepoStub) RevokeSession(context.Context, string, time.Time) error {
	if r.revokeErr != nil {
		return r.revokeErr
	}
	r.revoked = true
	r.active = false
	return nil
}
func (r *sessionRepoStub) SessionActive(_ context.Context, claims sessiontoken.Claims, _ time.Time) (bool, error) {
	r.claims = claims
	return r.active, nil
}

type sessionCacheStub struct {
	status          string
	statusErr       error
	statusCalls     int
	active, revoked bool
	setRevokedErr   error
	publishErr      error
	publishCalled   bool
}

func (c *sessionCacheStub) SetActive(context.Context, string, time.Duration) error {
	c.active = true
	c.status = sessiontoken.StatusActive
	return nil
}
func (c *sessionCacheStub) SetRevoked(context.Context, string, time.Duration) error {
	if c.setRevokedErr != nil {
		return c.setRevokedErr
	}
	c.active, c.revoked = false, true
	c.status = sessiontoken.StatusRevoked
	return nil
}
func (c *sessionCacheStub) Status(context.Context, string) (string, error) {
	c.statusCalls++
	if c.statusErr != nil {
		return "", c.statusErr
	}
	if c.status != "" {
		return c.status, nil
	}
	return sessiontoken.StatusMissing, nil
}
func (c *sessionCacheStub) PublishRevoked(context.Context, string) error {
	c.publishCalled = true
	return c.publishErr
}

func TestValidateSessionChecksPostgresOnCacheMissAndReturnsClaims(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	secret := []byte("a sufficiently long test signing secret 123456")
	want := sessiontoken.Claims{SessionID: "sid", UserID: "user", DeviceID: "device", IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Hour).Unix()}
	token, err := sessiontoken.Sign(want, secret)
	if err != nil {
		t.Fatal(err)
	}
	repo := &sessionRepoStub{active: true}
	cache := &sessionCacheStub{}
	service := app.New(repo, cache, app.Config{Secret: secret, Now: func() time.Time { return now }})
	request := httptest.NewRequest(http.MethodPost, "/internal/v1/session/validate", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	New(service).ServeHTTP(recorder, request)
	response := recorder.Result()
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", response.StatusCode)
	}
	var got sessiontoken.Claims
	if err := json.NewDecoder(response.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got != want || repo.claims != want || !cache.active || cache.statusCalls != 1 {
		t.Fatalf("claims=%+v postgres=%+v cache=%+v", got, repo.claims, cache)
	}
}

func TestLogoutDoesNotRequireRedisAuthentication(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	secret := []byte("a sufficiently long test signing secret 123456")
	claims := sessiontoken.Claims{SessionID: "sid", UserID: "user", DeviceID: "device", IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Hour).Unix()}
	token, err := sessiontoken.Sign(claims, secret)
	if err != nil {
		t.Fatal(err)
	}
	repo := &sessionRepoStub{active: true}
	cache := &sessionCacheStub{statusErr: errors.New("redis down")}
	service := app.New(repo, cache, app.Config{Secret: secret, Now: func() time.Time { return now }})
	request := httptest.NewRequest(http.MethodPost, "/v1/auth/logout", strings.NewReader(""))
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	New(service).ServeHTTP(recorder, request)
	response := recorder.Result()
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent || !repo.revoked || cache.statusCalls != 0 || !cache.publishCalled {
		t.Fatalf("status=%d repo=%+v cache=%+v", response.StatusCode, repo, cache)
	}
}
