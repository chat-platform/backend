// Package sessiontoken signs and verifies short-lived HS256 session JWTs.
package sessiontoken

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrInvalidToken = errors.New("invalid session token")

const StatusKeyPrefix = "session:status:"
const RevocationChannel = "session:revoked"
const (
	StatusActive  = "active"
	StatusRevoked = "revoked"
	StatusMissing = "missing"
)

type Claims struct {
	SessionID string `json:"sid"`
	UserID    string `json:"sub"`
	DeviceID  string `json:"device_id"`
	IssuedAt  int64  `json:"iat"`
	ExpiresAt int64  `json:"exp"`
}

type header struct {
	Algorithm string `json:"alg"`
	Type      string `json:"typ"`
}

func Sign(claims Claims, secret []byte) (string, error) {
	if len(secret) < 32 || claims.SessionID == "" || claims.UserID == "" || claims.DeviceID == "" || claims.ExpiresAt <= claims.IssuedAt {
		return "", ErrInvalidToken
	}
	encodedHeader, err := encode(header{Algorithm: "HS256", Type: "JWT"})
	if err != nil {
		return "", fmt.Errorf("encode token header: %w", err)
	}
	encodedClaims, err := encode(claims)
	if err != nil {
		return "", fmt.Errorf("encode token claims: %w", err)
	}
	unsigned := encodedHeader + "." + encodedClaims
	return unsigned + "." + signature(unsigned, secret), nil
}

func Verify(token string, secret []byte, now time.Time) (Claims, error) {
	var claims Claims
	if len(secret) < 32 {
		return claims, ErrInvalidToken
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return claims, ErrInvalidToken
	}
	expected := signature(parts[0]+"."+parts[1], secret)
	provided, err := base64.RawURLEncoding.DecodeString(parts[2])
	expectedBytes, _ := base64.RawURLEncoding.DecodeString(expected)
	if err != nil || !hmac.Equal(provided, expectedBytes) {
		return claims, ErrInvalidToken
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return claims, ErrInvalidToken
	}
	var tokenHeader header
	if json.Unmarshal(headerBytes, &tokenHeader) != nil || tokenHeader.Algorithm != "HS256" || tokenHeader.Type != "JWT" {
		return claims, ErrInvalidToken
	}
	claimBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || json.Unmarshal(claimBytes, &claims) != nil {
		return Claims{}, ErrInvalidToken
	}
	if claims.SessionID == "" || claims.UserID == "" || claims.DeviceID == "" || claims.IssuedAt <= 0 || claims.ExpiresAt <= now.Unix() || claims.IssuedAt > now.Add(time.Minute).Unix() || claims.ExpiresAt <= claims.IssuedAt {
		return Claims{}, ErrInvalidToken
	}
	return claims, nil
}

func encode(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func signature(value string, secret []byte) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(value))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
