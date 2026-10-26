package rmqkit

import (
	"context"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Message is the AMQP publication contract. The caller owns IDs, headers and body
// and must not mutate them until Produce returns. No IDs are generated.
type Message = amqp.Publishing

// Delivery contains message properties and routing information, without manual
// acknowledgement methods. rmqkit owns acknowledgement after the handler returns.
type Delivery struct {
	Message
	Redelivered bool
	Exchange    string
	RoutingKey  string
}

// Handler processes one delivery synchronously. Return nil to acknowledge, or an
// error to reject according to RequeueOnError. Honor ctx for bounded shutdown.
// The application owns handler-error reporting; consumption continues after rejection.
type Handler func(context.Context, Delivery) error

// Produce waits for a broker confirmation and reports mandatory routing failures.
// Retries cover transient setup failures only; once publishing starts an unknown
// outcome returns ErrPublishUncertain without resending. OperationTimeout bounds
// each attempt, including waiting for the publisher. Caller cancellation wins.
func (r *RabbitMQ) Produce(ctx context.Context, msg Message) error {
	if r.typ == ConsumerOnly {
		return ErrClientType
	}

	ctx, done, err := r.begin(ctx, true)

	if err != nil {
		return err
	}

	defer done()

	if msg.ContentType == "" {
		msg.ContentType = r.cfg.ContentType
	}

	if msg.DeliveryMode == 0 {
		msg.DeliveryMode = amqp.Transient

		if r.cfg.Persistent {
			msg.DeliveryMode = amqp.Persistent
		}
	}

	if err := msg.Headers.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}

	if msg.DeliveryMode != amqp.Transient && msg.DeliveryMode != amqp.Persistent {
		return fmt.Errorf("%w: invalid delivery mode", ErrInvalidMessage)
	}

	return r.withRetries(ctx, func() error { return r.produceAttempt(ctx, msg) })
}

func (r *RabbitMQ) produceAttempt(ctx context.Context, msg Message) error {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.OperationTimeout)
	defer cancel()

	if err := r.lock(ctx); err != nil {
		return err
	}

	defer r.unlock()

	if err := r.ensurePublisher(ctx); err != nil {
		return err
	}

	s := r.publisher
	stop := interruptOnCancel(ctx, s.raw)
	defer stop()

	confirmation, err := s.ch.PublishWithDeferredConfirm(r.cfg.Exchange, r.cfg.RoutingKey, true, false, msg)

	if err != nil {
		r.discard(s)
		r.publisher = nil

		return fmt.Errorf("%w: %w", ErrPublishUncertain, errors.Join(ctx.Err(), err))
	}

	acked, err := confirmation.WaitContext(ctx)

	if err != nil {
		r.discard(s)
		r.publisher = nil

		return fmt.Errorf("%w: %w", ErrPublishUncertain, err)
	}

	// RabbitMQ sends basic.return before confirmation; both notifications are
	// dispatched by the same reader, so the return is buffered by this point.
	select {
	case _, ok := <-s.returns:
		if ok {
			return ErrUnroutable
		}
	default:
	}

	if !acked {
		if s.conn.IsClosed() || s.ch.IsClosed() {
			r.discard(s)
			r.publisher = nil

			return fmt.Errorf("%w: %w", ErrPublishUncertain, amqp.ErrClosed)
		}

		return ErrPublishNack
	}

	return nil
}

// Consume blocks until ctx is cancelled, Close is called, or recovery fails.
// Each call owns a dedicated connection and invokes its handler sequentially.
// Subscription setup and settlement are bounded; the subscription itself lasts
// for the caller's context. Recovery permits Retries session restarts per call.
// Handler panics propagate to the caller after connection cleanup.
func (r *RabbitMQ) Consume(ctx context.Context, handler Handler) error {
	if r.typ == ProducerOnly {
		return ErrClientType
	}

	if handler == nil {
		return ErrInvalidHandler
	}

	ctx, done, err := r.begin(ctx, true)

	if err != nil {
		return err
	}

	defer done()

	return r.withRetries(ctx, func() error { return r.consumeSession(ctx, handler) })
}

func (r *RabbitMQ) consumeSession(ctx context.Context, handler Handler) error {
	s, err := r.openSession(ctx, false)

	if err != nil {
		return err
	}

	defer r.discard(s)

	stop := interruptOnCancel(ctx, s.raw)
	defer stop()

	closed := s.ch.NotifyClose(make(chan *amqp.Error, notificationBuffer))

	// Bound setup without lending a short-lived context to the subscription.
	setupCtx, cancel := context.WithTimeout(ctx, r.cfg.OperationTimeout)
	stopSetup := interruptOnCancel(setupCtx, s.raw)
	deliveries, err := s.ch.ConsumeWithContext(ctx, r.cfg.Queue, "", false, false, false, false, nil)
	stopSetup()
	setupErr := setupCtx.Err()

	if err != nil || setupErr != nil {
		err = contextError(setupCtx, "subscribe", errors.Join(err, setupErr))
	}

	cancel()

	if err != nil || setupErr != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delivery, ok := <-deliveries:
			if !ok {
				select {
				case cause := <-closed:
					if cause != nil {
						return cause
					}
				default:
				}

				return amqp.ErrClosed
			}

			if err := r.handleDelivery(ctx, s.raw, delivery, handler); err != nil {
				return err
			}
		}
	}
}

func (r *RabbitMQ) handleDelivery(ctx context.Context, raw interface{ Close() error }, d amqp.Delivery, handler Handler) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	msg := Delivery{
		Message: Message{
			Headers:         d.Headers,
			ContentType:     d.ContentType,
			ContentEncoding: d.ContentEncoding,
			DeliveryMode:    d.DeliveryMode,
			Priority:        d.Priority,
			CorrelationId:   d.CorrelationId,
			ReplyTo:         d.ReplyTo,
			Expiration:      d.Expiration,
			MessageId:       d.MessageId,
			Timestamp:       d.Timestamp,
			Type:            d.Type,
			UserId:          d.UserId,
			AppId:           d.AppId,
			Body:            d.Body,
		},
		Redelivered: d.Redelivered,
		Exchange:    d.Exchange,
		RoutingKey:  d.RoutingKey,
	}

	err := handler(ctx, msg)

	if ctx.Err() != nil {
		return ctx.Err()
	}

	settleCtx, cancel := context.WithTimeout(ctx, r.cfg.OperationTimeout)
	defer cancel()

	stop := interruptOnCancel(settleCtx, raw)
	defer stop()

	if err != nil {
		err = d.Nack(false, r.cfg.RequeueOnError)
	} else {
		err = d.Ack(false)
	}

	if err != nil || settleCtx.Err() != nil {
		return contextError(settleCtx, "settle delivery", err)
	}

	return nil
}
