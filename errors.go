package rmqkit

import "errors"

var (
	// ErrInvalidConfig identifies invalid configuration without exposing values.
	ErrInvalidConfig = errors.New("invalid RabbitMQ configuration")
	// ErrUninitialized means Connect has not completed successfully.
	ErrUninitialized = errors.New("RabbitMQ client is not connected")
	// ErrClosed means the client was permanently closed; construct a new client.
	ErrClosed = errors.New("RabbitMQ client is closed")
	// ErrClientType means the operation is disabled for this client type.
	ErrClientType = errors.New("operation is disabled for this client type")
	// ErrRetriesExceeded wraps the last transient error after all attempts fail.
	ErrRetriesExceeded = errors.New("RabbitMQ retry limit exceeded")
	// ErrUnroutable means the mandatory publish was returned by the broker.
	ErrUnroutable = errors.New("message is unroutable")
	// ErrPublishNack means the broker negatively acknowledged a publication.
	ErrPublishNack = errors.New("broker negatively acknowledged publication")
	// ErrPublishUncertain means a publish started but its outcome is unknown.
	// It is never retried automatically. The caller decides whether to resend.
	ErrPublishUncertain = errors.New("publication outcome is unknown")
	// ErrInvalidHandler means a consumer was called with a nil handler.
	ErrInvalidHandler = errors.New("consumer handler is required")
	// ErrTimeout identifies a configured transport timeout. Caller cancellation
	// and deadlines also remain available through errors.Is.
	ErrTimeout = errors.New("RabbitMQ operation timed out")
	// ErrInvalidMessage identifies invalid message settings or JSON conversion.
	ErrInvalidMessage = errors.New("invalid RabbitMQ message")
)
