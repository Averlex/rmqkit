package rmqkit

import "context"

// Logger represents an interface of logger visible to the app.
type Logger interface {
	// Info logs a message with level Info on the standard logger.
	Info(ctx context.Context, msg string, args ...any)
	// Debug logs a message with level Debug on the standard logger.
	Debug(ctx context.Context, msg string, args ...any)
	// Warn logs a message with level Warn on the standard logger.
	Warn(ctx context.Context, msg string, args ...any)
	// Error logs a message with level Error on the standard logger.
	Error(ctx context.Context, msg string, args ...any)
}
