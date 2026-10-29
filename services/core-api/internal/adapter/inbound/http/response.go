package httpadapter

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/chat-platform/backend/services/core-api/internal/app"
	"github.com/go-playground/validator/v10"
	"github.com/labstack/echo/v4"
)

type requestValidator struct {
	validator *validator.Validate
}

func newRequestValidator() echo.Validator {
	v := validator.New()
	if err := v.RegisterValidation("e164", func(field validator.FieldLevel) bool {
		phone, ok := field.Field().Interface().(string)
		return ok && app.IsE164Phone(phone)
	}); err != nil {
		panic(err)
	}
	return &requestValidator{validator: v}
}

func (v *requestValidator) Validate(value any) error {
	return v.validator.Struct(value)
}

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeJSON(c echo.Context, status int, value any) error {
	return c.JSON(status, value)
}

func writeError(c echo.Context, status int, code, message string) error {
	var body errorBody
	body.Error.Code = code
	body.Error.Message = message
	return writeJSON(c, status, body)
}

func decode(c echo.Context, target any) error {
	r := c.Request()
	r.Body = http.MaxBytesReader(c.Response(), r.Body, 16<<10)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode JSON request body: %w", err)
	}
	if err := c.Validate(target); err != nil {
		return fmt.Errorf("validate request body: %w", err)
	}
	return nil
}

func (s *Server) writeInvalidRequest(c echo.Context, err error) error {
	return s.writeExpectedError(c, "invalid request", err, http.StatusBadRequest, "invalid_request", "request body is invalid")
}

func (s *Server) writeExpectedError(c echo.Context, message string, cause error, status int, code, response string) error {
	s.logRequestError(c, message, cause, status, true)
	return writeError(c, status, code, response)
}

func (s *Server) writeServiceError(c echo.Context, err error) error {
	statusCode, code, message, debug := http.StatusInternalServerError, "internal_error", "request could not be completed", false
	switch {
	case errors.Is(err, app.ErrInvalidRequest):
		statusCode, code, message, debug = http.StatusBadRequest, "invalid_request", "request is invalid", true
	case errors.Is(err, app.ErrUnauthorized):
		statusCode, code, message, debug = http.StatusUnauthorized, "unauthorized", "authentication failed", true
	case errors.Is(err, app.ErrForbidden):
		statusCode, code, message, debug = http.StatusForbidden, "forbidden", "operation is not allowed", true
	case errors.Is(err, app.ErrConflict):
		statusCode, code, message, debug = http.StatusConflict, "conflict", "operation conflicts with current group state", true
	case errors.Is(err, app.ErrNotFound):
		statusCode, code, message, debug = http.StatusNotFound, "not_found", "resource was not found", true
	case errors.Is(err, app.ErrUnavailable):
		statusCode, code, message = http.StatusServiceUnavailable, "unavailable", "a required service is unavailable"
	}
	s.logRequestError(c, "request failed", err, statusCode, debug)
	return writeError(c, statusCode, code, message)
}

func (s *Server) logRequestError(c echo.Context, message string, err error, status int, debug bool) {
	if s.logger == nil {
		return
	}
	attrs := []any{"operation", c.Path(), "method", c.Request().Method, "status", status, "error", err}
	if debug {
		s.logger.Debug(c.Request().Context(), message, attrs...)
	} else {
		s.logger.Error(c.Request().Context(), message, attrs...)
	}
}
