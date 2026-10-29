package httpadapter

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/chat-platform/backend/services/core-api/internal/app"
	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/labstack/echo/v4"
)

func TestWriteServiceErrorLogsUnderlyingFailureOnceWithRequestContext(t *testing.T) {
	var output bytes.Buffer
	logger, err := logging.New(&output, logging.Config{Level: "debug", Format: "json"})
	if err != nil {
		t.Fatal(err)
	}
	e := echo.New()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/groups", nil)
	c := e.NewContext(request, recorder)
	c.SetPath("/v1/groups")
	s := &Server{echo: e, logger: logger}
	underlying := errors.New("postgres connection reset")

	if err := s.writeServiceError(c, fmt.Errorf("create group: %w", errors.Join(app.ErrUnavailable, underlying))); err != nil {
		t.Fatal(err)
	}

	logLine := strings.TrimSpace(output.String())
	if strings.Contains(logLine, "\n") {
		t.Fatalf("expected one log record, got %q", output.String())
	}
	for _, want := range []string{`"level":"ERROR"`, `"operation":"/v1/groups"`, `"method":"POST"`, `"status":503`, "postgres connection reset"} {
		if !strings.Contains(logLine, want) {
			t.Errorf("log %q does not contain %s", logLine, want)
		}
	}
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("response status=%d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
}

func TestWriteInvalidRequestLogsAtInfoWithoutRequestBody(t *testing.T) {
	var output bytes.Buffer
	logger, err := logging.New(&output, logging.Config{Level: "debug", Format: "json"})
	if err != nil {
		t.Fatal(err)
	}
	e := echo.New()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/auth/otp/verify", strings.NewReader(`{"code":"sensitive-code"}`))
	c := e.NewContext(request, recorder)
	c.SetPath("/v1/auth/otp/verify")
	s := &Server{echo: e, logger: logger}

	if err := s.writeInvalidRequest(c, errors.New("invalid request body")); err != nil {
		t.Fatal(err)
	}

	logLine := strings.TrimSpace(output.String())
	if !strings.Contains(logLine, `"level":"INFO"`) || !strings.Contains(logLine, `"status":400`) {
		t.Fatalf("expected info rejection log, got %q", logLine)
	}
	if strings.Contains(logLine, "sensitive-code") {
		t.Fatalf("log contains request body data: %q", logLine)
	}
}
