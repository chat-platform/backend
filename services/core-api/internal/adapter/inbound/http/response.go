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
	return &requestValidator{validator: validator.New()}
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

func writeInvalidRequest(c echo.Context, err error) error {
	c.Logger().Errorf("invalid request body: %v", err)
	return writeError(c, http.StatusBadRequest, "invalid_request", "request body is invalid")
}

func writeServiceError(c echo.Context, err error) error {
	switch {
	case errors.Is(err, app.ErrInvalidRequest):
		return writeError(c, http.StatusBadRequest, "invalid_request", "request is invalid")
	case errors.Is(err, app.ErrUnauthorized):
		return writeError(c, http.StatusUnauthorized, "unauthorized", "authentication failed")
	case errors.Is(err, app.ErrForbidden):
		return writeError(c, http.StatusForbidden, "forbidden", "operation is not allowed")
	case errors.Is(err, app.ErrConflict):
		return writeError(c, http.StatusConflict, "conflict", "operation conflicts with current group state")
	case errors.Is(err, app.ErrNotFound):
		return writeError(c, http.StatusNotFound, "not_found", "resource was not found")
	case errors.Is(err, app.ErrUnavailable):
		return writeError(c, http.StatusServiceUnavailable, "unavailable", "a required service is unavailable")
	default:
		return writeError(c, http.StatusInternalServerError, "internal_error", "request could not be completed")
	}
}
