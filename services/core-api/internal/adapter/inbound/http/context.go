package httpadapter

import (
	"context"
	"net/http"

	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
)

type identityKey struct{}

// Kept as a small context helper to avoid exposing auth claims to handlers through request fields.
func contextWithClaims(r *http.Request, claims sessiontoken.Claims) context.Context {
	return context.WithValue(r.Context(), identityKey{}, claims)
}

func claimsFrom(r *http.Request) sessiontoken.Claims {
	claims, _ := r.Context().Value(identityKey{}).(sessiontoken.Claims)
	return claims
}
