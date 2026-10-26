package rmqkit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (s *ClientSuite) TestRetryClassification() {
	tests := []struct {
		name  string
		err   error
		retry bool
	}{
		{"EOF", fmt.Errorf("wrapped: %w", io.EOF), true},
		{"refused", syscall.ECONNREFUSED, true},
		{"closed channel", amqp.ErrClosed, true},
		{"broker restart", &amqp.Error{Code: amqp.ConnectionForced}, true},
		{"authentication", amqp.ErrCredentials, false},
		{"missing queue", &amqp.Error{Code: amqp.NotFound}, false},
		{"topology mismatch", &amqp.Error{Code: amqp.PreconditionFailed}, false},
		{"cancelled", context.Canceled, false},
		{"caller deadline", context.DeadlineExceeded, false},
		{"configured timeout", errors.Join(ErrTimeout, context.DeadlineExceeded), true},
		{"uncertain", errors.Join(ErrPublishUncertain, io.EOF), false},
		{"unroutable", ErrUnroutable, false},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.Equal(tt.retry, isRetryable(tt.err))
		})
	}
}

func (s *ClientSuite) TestRetriesReturnFinalFailure() {
	const retries = 2

	cfg := testConfig()
	cfg.Retries = retries
	cfg.RetryDelay = time.Nanosecond
	r := s.newClient(cfg, FullClient)
	attempts := 0

	err := r.withRetries(context.Background(), func() error {
		attempts++

		return io.EOF
	})

	s.Equal(retries+1, attempts)
	s.ErrorIs(err, ErrRetriesExceeded)
	s.ErrorIs(err, io.EOF)
}

func (s *ClientSuite) TestRetriesStopOnPermanentFailureAndSuccess() {
	tests := []struct {
		name     string
		result   error
		attempts int
	}{
		{"permanent", amqp.ErrCredentials, 1},
		{"success", nil, 2},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			cfg := testConfig()
			cfg.RetryDelay = time.Nanosecond
			r := s.newClient(cfg, FullClient)
			attempts := 0

			err := r.withRetries(context.Background(), func() error {
				attempts++

				if tt.result == nil && attempts == 1 {
					return io.EOF
				}

				return tt.result
			})

			s.Equal(tt.attempts, attempts)
			s.ErrorIs(err, tt.result)
		})
	}
}

func (s *ClientSuite) TestRetryDelayHonorsCancellation() {
	cfg := testConfig()
	cfg.RetryDelay = time.Hour
	r := s.newClient(cfg, FullClient)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	attempted := make(chan struct{})
	result := make(chan error, 1)

	go func() {
		result <- r.withRetries(ctx, func() error {
			close(attempted)

			return io.EOF
		})
	}()

	select {
	case <-attempted:
	case <-ctx.Done():
		s.Require().NoError(ctx.Err(), "operation never attempted")
	}

	cancel()

	select {
	case err := <-result:
		s.ErrorIs(err, context.Canceled)
	case <-time.After(testTimeout):
		s.FailNow("retry did not stop")
	}
}
