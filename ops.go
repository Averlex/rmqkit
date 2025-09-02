package rmqkit

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Produce sends a message to the message queue using retry logic and operation timeout.
func (r *RabbitMQ) Produce(ctx context.Context, payload []byte) error {
	err := r.withRetries(ctx, "produce", func() error {
		return r.withTimeout(ctx, func(localCtx context.Context) error {
			r.mu.Lock()
			err := r.ch.PublishWithContext(
				localCtx,
				r.topic,
				r.routingKey,
				false, // mandatory
				false, // immediate
				amqp.Publishing{
					ContentType: r.contentType,
					Body:        payload,
					MessageId:   uuid.New().String(),
				},
			)
			r.mu.Unlock()
			return err
		})
	})
	if err != nil {
		return err
	}
	r.l.Debug(ctx, "message successfully sent")
	return nil
}

// Consume consumes messages from the message queue using retry logic and operation timeout.
// The methods abstracts the logic of consuming messages from the message queue.
func (r *RabbitMQ) Consume(ctx context.Context) (<-chan []byte, <-chan error) {
	resQueue := make(chan []byte)
	errors := make(chan error)

	r.mu.RLock()
	requeue := r.requeue
	autoAck := r.autoAck
	resubTimeout := r.resubTimeout
	r.mu.RUnlock()

	// Looping until the context is cancelled.
	// The loop updates the subscription and populates the consumer with data until the subscription ends.
	go func() {
		defer close(errors)
		defer close(resQueue)

		for {
			// Updating the subscription.
			ch, err := r.startConsumer(ctx)
			if err != nil {
				select {
				case errors <- err:
				case <-ctx.Done():
				}
				return
			}
			// Populating the consumer until the subscription ends or context cancellation.
			r.populateConsumer(ctx, ch, resQueue, autoAck, requeue)

			select {
			case <-ctx.Done():
				r.l.Warn(ctx, "context done before during consuming process")
				return
			case <-time.After(resubTimeout):
				continue
			}
		}
	}()

	return resQueue, errors
}

// populateConsumer reads messages from the AMPQ channel and sends raw data to the result channel.
func (r *RabbitMQ) populateConsumer(
	ctx context.Context,
	ch <-chan amqp.Delivery,
	resQueue chan<- []byte,
	autoAck,
	requeue bool,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				// Consumer needs to be resubscribed.
				return
			}
			if !autoAck {
				if err := msg.Ack(false); err != nil {
					r.l.Warn(
						ctx,
						"message ack failed",
						slog.String("message_id", msg.MessageId),
						slog.Any("error", err),
					)
					err = msg.Reject(requeue)
					if err != nil {
						r.l.Error(
							ctx,
							"message reject failed",
							slog.String("message_id", msg.MessageId),
							slog.Any("error", err),
						)
						continue
					}
				}
			}
			select {
			case <-ctx.Done():
				r.l.Warn(ctx, "context done before the message extracted")
			case resQueue <- msg.Body:
				r.l.Debug(ctx, "message successfully received", slog.String("message_id", msg.MessageId))
			}
		}
	}
}

// startConsumer starts a new consumer using retry logic and operation timeout.
func (r *RabbitMQ) startConsumer(ctx context.Context) (<-chan amqp.Delivery, error) {
	var err error
	var ch <-chan amqp.Delivery
	var autoAck bool

	err = r.withRetries(ctx, "consume", func() error {
		return r.withTimeout(ctx, func(localCtx context.Context) error {
			r.mu.Lock()
			defer r.mu.Unlock()

			autoAck = r.autoAck
			ch, err = r.ch.ConsumeWithContext(
				localCtx,
				r.topic,
				"",
				autoAck,
				false, // exclusive
				false, // noLocal
				false, // noWait
				nil,   // args
			)
			return err
		})
	})
	if err != nil {
		return nil, fmt.Errorf("subscribe consumer: %w", err)
	}

	return ch, nil
}
