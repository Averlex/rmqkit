package rmqkit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func isRetryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) ||
		errors.Is(err, ErrPublishUncertain) {
		return false
	}

	if errors.Is(err, ErrTimeout) {
		return true
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	if errors.Is(err, amqp.ErrClosed) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EPIPE) {
		return true
	}

	var protocolErr *amqp.Error

	if errors.As(err, &protocolErr) {
		return protocolErr.Code == amqp.ConnectionForced || protocolErr.Code == amqp.InternalError
	}

	var netErr net.Error

	return errors.As(err, &netErr) && netErr.Timeout()
}

func (r *RabbitMQ) withRetries(ctx context.Context, fn func() error) error {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := fn()

		if ctx.Err() != nil {
			return errors.Join(ctx.Err(), err)
		}

		if err == nil || !isRetryable(err) {
			return err
		}

		if attempt == r.cfg.Retries {
			return fmt.Errorf("%w: %w", ErrRetriesExceeded, err)
		}

		timer := time.NewTimer(r.cfg.RetryDelay)

		select {
		case <-ctx.Done():
			timer.Stop()

			return ctx.Err()
		case <-timer.C:
		}
	}
}
