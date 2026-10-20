package coreapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestValidateSessionSendsBearerAndDecodesClaims(t *testing.T) {
	want := sessiontoken.Claims{SessionID: "sid", UserID: "user", DeviceID: "device", IssuedAt: 10, ExpiresAt: 20}
	validator := NewSessionValidator("core-api:8081")
	validator.client = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPost || request.URL.String() != "http://core-api:8081/internal/v1/session/validate" {
			t.Errorf("request=%s %s", request.Method, request.URL)
		}
		if got := request.Header.Get("Authorization"); got != "Bearer signed-token" {
			t.Errorf("authorization=%q", got)
		}
		body, _ := json.Marshal(want)
		return response(request, http.StatusOK, string(body)), nil
	})}

	got, err := validator.ValidateSession(context.Background(), "signed-token")
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("claims=%+v, want %+v", got, want)
	}
}

func TestValidateSessionFailsClosedOnCoreAPIError(t *testing.T) {
	validator := NewSessionValidator("core-api:8081")
	validator.client = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return response(request, http.StatusUnauthorized, "revoked"), nil
	})}
	if _, err := validator.ValidateSession(context.Background(), "signed-token"); err != domain.ErrInvalidIdentity {
		t.Fatalf("Core API rejection error=%v, want invalid identity", err)
	}
}

func TestValidateSessionHonorsRequestContext(t *testing.T) {
	validator := NewSessionValidator("core-api:8081")
	validator.client = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return nil, request.Context().Err()
	})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := validator.ValidateSession(ctx, "signed-token"); err == nil {
		t.Fatal("cancelled request unexpectedly succeeded")
	}
}

func response(request *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}
}
