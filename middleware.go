package rmqkit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// withTimeout wraps the given function in a context.WithTimeout call.
func (r *RabbitMQ) withTimeout(ctx context.Context, fn func(context.Context) error) error {
	r.mu.RLock()
	if r.ch == nil {
		r.mu.RUnlock()
		return ErrUninitialized
	}
	timeout := r.timeout
	r.mu.RUnlock()

	localCtx := ctx
	var cancel context.CancelFunc

	if timeout > 0 {
		localCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	err := fn(localCtx)
	if err != nil {
		if errors.Is(localCtx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("%w: %w", errTimeoutExceeded, err)
		}
		return err
	}
	return nil
}

// withRetries is a middleware that implements retry logic for RabbitMQ calls.
//
// It tries to execute the given function with configured retries and timeout.
// Each attempt is logged with DEBUG level.
//
// If the connection is unavailable, it will try to reconnect.
// Each fact of unavailability is logged with ERROR, each reconnection attempt - with INFO.
//
// Receiving any other error stops execution and returns the error.
//
// If retry limit is exceeded, ErrRetriesExceeded is returned, wrapped over the last error.
func (r *RabbitMQ) withRetries(ctx context.Context, method string, fn func() error) error {
	var err error
	r.mu.RLock()
	attempts := r.retries + 1 // Guarantees at least one attempt.
	timeout := r.retryTimeout
	r.mu.RUnlock()

	for i := 0; i < attempts; i++ {
		err = fn()
		if err == nil {
			return nil
		}

		if !isRetryable(err) {
			r.l.Error(
				ctx,
				"unexpected error occurred",
				slog.String("method", method),
				slog.Any("error", err),
			)
		}

		r.l.Debug(
			ctx,
			"operation failed, attempting to reconnect",
			slog.String("method", method),
			slog.Int("attempt", i+1),
			slog.Any("error", err),
		)

		// Closing and reestablishing connection.
		_ = r.Close(ctx)
		if err = r.Connect(ctx); err != nil {
			r.l.Error(
				ctx,
				"reestablishing connection",
				slog.String("method", method),
				slog.Int("attempt", i+1),
				slog.Any("error", err),
			)
			return fmt.Errorf("%w: %w", ErrFatal, err)
		}

		// No wait on last attempt.
		if i < attempts-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(timeout):
				continue
			}
		}
	}

	return nil
}
