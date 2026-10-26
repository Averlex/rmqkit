package rmqkit

import (
	"context"
	"encoding/json"
	"fmt"
)

// Client publishes and subscribes to JSON values of an application-owned type.
// Its ordinary methods can satisfy interfaces declared by the application.
// The underlying RabbitMQ owns connections and lifecycle; wrappers add no
// connections or background work and are safe to share between goroutines.
type Client[T any] struct {
	transport messageTransport
}

// messageTransport is the transport contract consumed by the typed JSON client.
type messageTransport interface {
	Produce(context.Context, Message) error
	Consume(context.Context, Handler) error
}

// Typed wraps a transport without connecting or taking ownership of it. Multiple
// wrappers share its configured queue/exchange binding and publisher connection.
// Connect and Close the transport at the application boundary. A nil transport
// produces a client whose operations return ErrUninitialized.
func Typed[T any](transport *RabbitMQ) *Client[T] {
	if transport == nil {
		return &Client[T]{}
	}

	return &Client[T]{transport: transport}
}

// Publish JSON-encodes the whole value and waits for confirmed publication.
// No field names or envelope layout are required. Standard encoding/json rules
// apply, including base64 encoding for []byte and null for nil pointers.
// Serialization failures wrap ErrInvalidMessage; transport errors are preserved.
// IDs in the value remain application data and are not copied to AMQP properties.
// Do not mutate message while Publish is running.
func (c *Client[T]) Publish(ctx context.Context, message T) error {
	if c == nil || c.transport == nil {
		return ErrUninitialized
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	body, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("%w: encode JSON: %w", ErrInvalidMessage, err)
	}

	return c.transport.Produce(ctx, Message{Body: body, ContentType: DefaultContentType})
}

// Subscribe blocks while consuming JSON values and invoking handler sequentially.
// Cancel ctx from outside to stop it. Reconnection, resubscription, prefetch and
// shutdown behavior are inherited from RabbitMQ. Successful decoding and a nil
// handler result acknowledge the delivery. Decode failures wrap ErrInvalidMessage
// and use the same rejection policy as handler failures; consumption continues.
// The application owns handler-error logging. Handler panics propagate.
func (c *Client[T]) Subscribe(ctx context.Context, handler func(context.Context, T) error) error {
	if handler == nil {
		return ErrInvalidHandler
	}

	if c == nil || c.transport == nil {
		return ErrUninitialized
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	return c.transport.Consume(ctx, func(ctx context.Context, delivery Delivery) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		var message T

		if err := json.Unmarshal(delivery.Body, &message); err != nil {
			return fmt.Errorf("%w: decode JSON: %w", ErrInvalidMessage, err)
		}

		return handler(ctx, message)
	})
}
