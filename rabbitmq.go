// Package rmqkit provides confirmed publishing and handler-based RabbitMQ consumption.
package rmqkit

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ClientType restricts the operations a client can perform.
type ClientType int

// RabbitMQ owns its connections. Methods are safe to call concurrently. Each
// Consume call owns a separate connection; publishing is serialized. Call Close
// to cancel active operations and wait for handlers to return.
type RabbitMQ struct {
	cfg Config
	typ ClientType

	mu        sync.Mutex
	closed    bool
	connected bool
	active    int
	idle      chan struct{}
	sockets   map[net.Conn]struct{}
	lifetime  context.Context
	cancel    context.CancelFunc
	gate      chan struct{}

	// Accessed only while gate is held.
	publisher *session
}

type session struct {
	raw     net.Conn
	conn    *amqp.Connection
	ch      *amqp.Channel
	returns <-chan amqp.Return
}

// NewRabbitMQ validates cfg without doing I/O or configuring logging.
// Initialize cfg with DefaultConfig before overriding fields.
func NewRabbitMQ(cfg Config, typ ClientType) (*RabbitMQ, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if typ < FullClient || typ > ConsumerOnly {
		return nil, fmt.Errorf("%w: unknown client type", ErrInvalidConfig)
	}

	if cfg.TLSConfig != nil {
		cfg.TLSConfig = cfg.TLSConfig.Clone()
	}

	ctx, cancel := context.WithCancel(context.Background())
	idle := make(chan struct{})
	close(idle)

	return &RabbitMQ{
		cfg:      cfg,
		typ:      typ,
		idle:     idle,
		sockets:  make(map[net.Conn]struct{}),
		lifetime: ctx,
		cancel:   cancel,
		gate:     make(chan struct{}, publisherConcurrency),
	}, nil
}

// Connect establishes a connection and validates or declares topology. Repeated
// calls are idempotent while connected. Close permanently disables the client.
func (r *RabbitMQ) Connect(ctx context.Context) error {
	ctx, done, err := r.begin(ctx, false)

	if err != nil {
		return err
	}

	defer done()

	if err := r.lock(ctx); err != nil {
		return err
	}

	defer r.unlock()

	err = r.withRetries(ctx, func() error { return r.ensurePublisher(ctx) })

	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrClosed
	}

	r.connected = true

	return nil
}

// Close permanently closes the client, cancels operations and waits for owned
// work within ctx's deadline. A handler must honor its context. If ctx expires,
// sockets remain closed; a subsequent Close can wait again. Do not call Close
// synchronously from a handler, because Close waits for that handler to return.
// Unacknowledged deliveries are left for broker redelivery.
func (r *RabbitMQ) Close(ctx context.Context) error {
	r.mu.Lock()
	r.closed = true
	r.cancel()

	sockets := make([]net.Conn, 0, len(r.sockets))

	for socket := range r.sockets {
		sockets = append(sockets, socket)
	}

	idle := r.idle
	r.mu.Unlock()

	var errs []error

	for _, socket := range sockets {
		if err := socket.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
	}

	select {
	case <-idle:
		return errors.Join(errs...)
	case <-ctx.Done():
		return errors.Join(append(errs, ctx.Err())...)
	}
}

func (r *RabbitMQ) begin(ctx context.Context, requireConnected bool) (context.Context, func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil, nil, ErrClosed
	}

	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	if requireConnected && !r.connected {
		return nil, nil, ErrUninitialized
	}

	if r.active == 0 {
		r.idle = make(chan struct{})
	}

	r.active++
	localCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.lifetime, cancel)

	return localCtx, func() {
		stop()
		cancel()

		r.mu.Lock()
		defer r.mu.Unlock()

		r.active--

		if r.active == 0 {
			close(r.idle)
		}
	}, nil
}

func (r *RabbitMQ) lock(ctx context.Context) error {
	select {
	case r.gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			r.unlock()

			return err
		}

		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *RabbitMQ) unlock() {
	<-r.gate
}

func (r *RabbitMQ) ensurePublisher(ctx context.Context) error {
	if r.publisher != nil && !r.publisher.conn.IsClosed() && !r.publisher.ch.IsClosed() {
		return nil
	}

	r.discard(r.publisher)
	r.publisher = nil

	s, err := r.openSession(ctx, r.typ != ConsumerOnly)

	if err != nil {
		return err
	}

	r.publisher = s

	return nil
}

func (r *RabbitMQ) discard(s *session) {
	if s == nil {
		return
	}

	_ = s.raw.Close()

	r.mu.Lock()
	delete(r.sockets, s.raw)
	r.mu.Unlock()
}

// Closing the owned socket interrupts dependency calls that ignore context.
func interruptOnCancel(ctx context.Context, raw interface{ Close() error }) func() {
	finished := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = raw.Close()
		close(finished)
	})

	return func() {
		if !stop() {
			<-finished
		}
	}
}

func (r *RabbitMQ) openSession(ctx context.Context, publishing bool) (_ *session, err error) {
	setupCtx, cancel := context.WithTimeout(ctx, r.cfg.ConnectTimeout)
	defer cancel()

	port := r.cfg.Port

	if port == 0 {
		port = DefaultPort

		if r.cfg.TLS {
			port = DefaultTLSPort
		}
	}

	raw, err := (&net.Dialer{}).DialContext(setupCtx, networkTCP, net.JoinHostPort(r.cfg.Host, strconv.Itoa(port)))

	if err != nil {
		return nil, contextError(setupCtx, "dial broker", err)
	}

	s := &session{raw: raw}
	r.mu.Lock()

	if r.closed {
		r.mu.Unlock()
		_ = raw.Close()

		return nil, ErrClosed
	}

	r.sockets[raw] = struct{}{}
	r.mu.Unlock()
	defer func() {
		if err != nil {
			r.discard(s)
		}
	}()

	stop := interruptOnCancel(setupCtx, raw)
	defer stop()

	var transport net.Conn = raw

	if r.cfg.TLS {
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: r.cfg.Host}

		if r.cfg.TLSConfig != nil {
			tlsCfg = r.cfg.TLSConfig.Clone()

			if tlsCfg.ServerName == "" {
				tlsCfg.ServerName = r.cfg.Host
			}
		}

		secure := tls.Client(raw, tlsCfg)

		if err = secure.HandshakeContext(setupCtx); err != nil {
			return nil, contextError(setupCtx, "TLS handshake", err)
		}

		transport = secure
	}

	s.conn, err = amqp.Open(transport, amqp.Config{
		SASL:      []amqp.Authentication{&amqp.PlainAuth{Username: r.cfg.Username, Password: r.cfg.Password}},
		Vhost:     r.cfg.Vhost,
		Heartbeat: heartbeatInterval,
		Locale:    amqpLocale,
	})

	if err != nil {
		return nil, contextError(setupCtx, "AMQP handshake", err)
	}

	s.ch, err = s.conn.Channel()

	if err != nil {
		return nil, contextError(setupCtx, "open channel", err)
	}

	if err = r.setupTopology(s.ch); err != nil {
		return nil, contextError(setupCtx, "topology", err)
	}

	if publishing {
		s.returns = s.ch.NotifyReturn(make(chan amqp.Return, notificationBuffer))
		err = s.ch.Confirm(false)
	} else {
		err = s.ch.Qos(r.cfg.Prefetch, 0, false)
	}

	if err != nil {
		return nil, contextError(setupCtx, "channel setup", err)
	}

	if setupCtx.Err() != nil {
		return nil, contextError(setupCtx, "channel setup", setupCtx.Err())
	}

	return s, nil
}

func contextError(ctx context.Context, op string, err error) error {
	cause := errors.Join(ctx.Err(), err)

	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		cause = errors.Join(ErrTimeout, cause)
	}

	return fmt.Errorf("%s: %w", op, cause)
}
