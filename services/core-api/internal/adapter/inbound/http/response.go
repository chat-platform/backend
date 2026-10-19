package httpadapter

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/chat-platform/backend/services/core-api/internal/app"
	"github.com/labstack/echo/v4"
)

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

func decode(c echo.Context, target any) bool {
	r := c.Request()
	r.Body = http.MaxBytesReader(c.Response(), r.Body, 16<<10)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil {
		_ = writeError(c, http.StatusBadRequest, "invalid_request", "request body is invalid")
		return false
	}
	return true
}

func writeServiceError(c echo.Context, err error) error {
	switch {
	case errors.Is(err, app.ErrInvalidRequest):
		return writeError(c, http.StatusBadRequest, "invalid_request", "request is invalid")
	case errors.Is(err, app.ErrUnauthorized):
		return writeError(c, http.StatusUnauthorized, "unauthorized", "authentication failed")
	case errors.Is(err, app.ErrNotFound):
		return writeError(c, http.StatusNotFound, "not_found", "resource was not found")
	case errors.Is(err, app.ErrUnavailable):
		return writeError(c, http.StatusServiceUnavailable, "unavailable", "authentication state is unavailable")
	default:
		return writeError(c, http.StatusInternalServerError, "internal_error", "request could not be completed")
	}
}
