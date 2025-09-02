package rmqkit

import (
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// mapToFullClient creates a new map with default values for FullClient configuration.
func mapToFullClient(cfg map[string]any) map[string]any {
	newCfg := make(map[string]any, len(expectedFieldsFull))
	for k := range expectedFieldsFull {
		// Use values from config if they are present.
		if v, ok := cfg[k]; ok {
			newCfg[k] = v
			continue
		}
		// Null value otherwise.
		newCfg[k] = expectedFieldsFull[k]
	}
	return newCfg
}

// isQueueExists checks if the queue exists.
//
// Method does not check for channel existanse, as well as it does not uses locks.
//
// Returns:
// - (true, nil): queue exists.
// - (false, nil): queue does not exist.
// - (false, error): some internal error occurred.
func (r *RabbitMQ) isQueueExists() (bool, error) {
	if _, err := r.ch.QueueDeclarePassive(
		r.topic,
		r.durable,
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil,   // args
	); err != nil {
		var amqpErr *amqp.Error
		if errors.As(err, &amqpErr) {
			if amqpErr.Code == 404 {
				return false, nil
			}
		}
		return false, fmt.Errorf("message queue check: %w", err)
	}

	return true, nil
}

// initQueueExchange creates a new exchange and queue and binds them.
// If the channel and/or queue exists it returns error only in case of setup mismatch, ignores creation otherwise.
//
// Method does not check for channel existanse, as well as it does not uses locks.
func (r *RabbitMQ) initQueueExchange() error {
	var err error
	if err = r.ch.ExchangeDeclare(
		r.topic,
		"direct",
		r.durable,
		false, // autoDelete
		false, // internal
		false, // noWait
		nil,   // args
	); err != nil {
		return fmt.Errorf("message queue exchange declaration: %w", err)
	}

	var q amqp.Queue
	if q, err = r.ch.QueueDeclare(
		r.topic,
		r.durable,
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil,   // args
	); err != nil {
		return fmt.Errorf("message queue declaration: %w", err)
	}

	if err = r.ch.QueueBind(
		q.Name,
		r.routingKey,
		r.topic,
		false, // noWait
		nil,   // args
	); err != nil {
		return fmt.Errorf("message queue binding: %w", err)
	}

	return nil
}
