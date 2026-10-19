package coreapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
)

type SessionValidator struct {
	baseURL string
	client  *http.Client
}

func NewSessionValidator(address string) *SessionValidator {
	address = strings.TrimRight(address, "/")
	if !strings.HasPrefix(address, "http://") && !strings.HasPrefix(address, "https://") {
		address = "http://" + address
	}
	return &SessionValidator{
		baseURL: address,
		client:  &http.Client{Timeout: 3 * time.Second},
	}
}

func (v *SessionValidator) ValidateSession(ctx context.Context, token string) (sessiontoken.Claims, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, v.baseURL+"/internal/v1/session/validate", nil)
	if err != nil {
		return sessiontoken.Claims{}, fmt.Errorf("create Core API session validation request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := v.client.Do(request)
	if err != nil {
		return sessiontoken.Claims{}, fmt.Errorf("validate session with Core API: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
			return sessiontoken.Claims{}, domain.ErrInvalidIdentity
		}
		return sessiontoken.Claims{}, fmt.Errorf("Core API session validation returned %s", response.Status)
	}
	var claims sessiontoken.Claims
	if err := json.NewDecoder(response.Body).Decode(&claims); err != nil {
		return sessiontoken.Claims{}, fmt.Errorf("decode Core API session validation response: %w", err)
	}
	return claims, nil
}
