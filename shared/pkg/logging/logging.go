package logging

import (
	"context"
	"io"
	"log/slog"
	"strings"
)

// Logger is the service-wide logging contract. Implementations can add shared
// controls without requiring callers to depend on a specific logging backend.
type Logger interface {
	Debug(context.Context, string, ...any)
	Info(context.Context, string, ...any)
	Warn(context.Context, string, ...any)
	Error(context.Context, string, ...any)
	With(...any) Logger
}

type Config struct {
	Level     string
	Format    string
	AddSource bool
}

type slogLogger struct {
	logger *slog.Logger
}

func New(output io.Writer, config Config) (Logger, error) {
	level, err := parseLevel(config.Level)
	if err != nil {
		return nil, err
	}

	options := &slog.HandlerOptions{Level: level, AddSource: config.AddSource}
	var handler slog.Handler
	switch strings.ToLower(strings.TrimSpace(config.Format)) {
	case "", "text":
		handler = slog.NewTextHandler(output, options)
	case "json":
		handler = slog.NewJSONHandler(output, options)
	default:
		return nil, &ConfigError{Field: "format", Value: config.Format}
	}
	return &slogLogger{logger: slog.New(handler)}, nil
}

func parseLevel(value string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, &ConfigError{Field: "level", Value: value}
	}
}

type ConfigError struct {
	Field string
	Value string
}

func (e *ConfigError) Error() string {
	return "invalid logging " + e.Field + ": " + e.Value
}

func (l *slogLogger) Debug(ctx context.Context, message string, args ...any) {
	l.logger.DebugContext(ctx, message, args...)
}

func (l *slogLogger) Info(ctx context.Context, message string, args ...any) {
	l.logger.InfoContext(ctx, message, args...)
}

func (l *slogLogger) Warn(ctx context.Context, message string, args ...any) {
	l.logger.WarnContext(ctx, message, args...)
}

func (l *slogLogger) Error(ctx context.Context, message string, args ...any) {
	l.logger.ErrorContext(ctx, message, args...)
}

func (l *slogLogger) With(args ...any) Logger {
	return &slogLogger{logger: l.logger.With(args...)}
}
