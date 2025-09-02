package rmqkit

import (
	"errors"
)

var (
	// ErrUninitialized is returned when the message queue connection is not initialized.
	ErrUninitialized = errors.New("rabbitmq is not initialized (initialize connection first?)")
	// ErrFatal is returned when an unexpected internal error occurs.
	ErrFatal = errors.New("fatal error")
)

// errTimeoutExceeded is returned when the operation execution times out. Internal package error.
var errTimeoutExceeded = errors.New("timeout exceeded")
