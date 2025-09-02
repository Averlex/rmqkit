// Package rmqkit provides a RabbitMQ client, which is suitable for sending and receiving messages.
// It is expexted to work with a single queue.
package rmqkit

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ClientType represents a RabbitMQ client type.
type ClientType int

const (
	// FullClient matches a full client configuration.
	FullClient ClientType = iota
	// ProducerOnly matches a client configuration with producer-only fields.
	ProducerOnly
	// ConsumerOnly matches a client configuration with consumer-only fields.
	ConsumerOnly
)

// RabbitMQ represents a RabbitMQ client.
type RabbitMQ struct {
	mu sync.RWMutex

	conn       *amqp.Connection
	ch         *amqp.Channel
	clientType ClientType

	url          string
	timeout      time.Duration
	retryTimeout time.Duration
	retries      int

	topic       string
	durable     bool
	contentType string

	routingKey string

	resubTimeout time.Duration // Resubscription timeout for the consumer.
	autoAck      bool
	requeue      bool

	l Logger
}

// NewRabbitMQ creates a new RabbitMQ client.
// Supports partial configuration for different client types: consumer, producer or full client.
// Unknown client type defaults to FullClient.
func NewRabbitMQ(logger Logger, cfg map[string]any, typ ClientType) (*RabbitMQ, error) {
	// Args validation.
	if cfg == nil {
		return nil, fmt.Errorf("no configuration passed to RabbitMQ constructor")
	}
	missing, wrongType := make([]string, 0), make([]string, 0)
	if logger == nil {
		missing = append(missing, "logger")
	}

	// Field types validation.
	var reqFields map[string]any
	switch typ {
	case FullClient:
		reqFields = expectedFieldsFull
	case ProducerOnly:
		reqFields = expectedFieldsProducer
	case ConsumerOnly:
		reqFields = expectedFieldsConsumer
	default:
		typ = FullClient
		reqFields = expectedFieldsFull
	}

	m, w := validateFields(cfg, reqFields)
	missing, wrongType = append(missing, m...), append(wrongType, w...)
	if len(missing) > 0 || len(wrongType) > 0 {
		return nil, fmt.Errorf("invalid RabbitMQ config: missing=%v invalid_type=%v",
			missing, wrongType)
	}

	// Extract from config an normalize the value.
	config := mapToFullClient(cfg)

	retryTimeout, _ := config["retry_timeout"].(time.Duration)
	if retryTimeout <= 0 {
		return nil, fmt.Errorf("invalid config data: retry timeout must be positive, got %v", retryTimeout)
	}
	resubTimeout, _ := config["resub_timeout"].(time.Duration)
	if resubTimeout <= 0 && typ != ProducerOnly {
		return nil, fmt.Errorf("invalid config data: resubscription timeout must be positive, got %v", resubTimeout)
	}

	// Init the full version regardless of the client type.
	return &RabbitMQ{
		l:          logger,
		clientType: typ,
		url: fmt.Sprintf(
			"amqp://%s:%s@%s:%s/",
			config["user"].(string),
			config["password"].(string),
			config["host"].(string),
			config["port"].(string),
		),
		timeout:      config["timeout"].(time.Duration),
		retryTimeout: retryTimeout,
		retries:      max(config["retries"].(int), 0),
		topic:        config["topic"].(string),
		durable:      config["durable"].(bool),
		contentType:  config["content_type"].(string),
		routingKey:   config["routing_key"].(string),
		autoAck:      config["auto_ack"].(bool),
		requeue:      config["requeue"].(bool),
		resubTimeout: config["resub_timeout"].(time.Duration),
	}, nil
}

// Connect to the message queue.
func (r *RabbitMQ) Connect(_ context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	conn, err := amqp.Dial(r.url)
	if err != nil {
		return fmt.Errorf("message queue connection: %w", err)
	}
	r.conn = conn

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("message queue channel creation: %w", err)
	}
	r.ch = ch

	// Consumer-only logic shortcut.
	if r.clientType == ConsumerOnly {
		ok, err := r.isQueueExists()
		if err != nil {
			return fmt.Errorf("unexpected error on consumer message queue check: %w", err)
		}
		if ok {
			return nil
		}
		return fmt.Errorf("consumer message queue does not exist")
	}

	// Producer-only logic follows the full client scenario.
	err = r.initQueueExchange()
	if err != nil {
		return fmt.Errorf("producer message queue init: %w", err)
	}

	return nil
}

// Close the connection to the message queue.
func (r *RabbitMQ) Close(_ context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.conn == nil && r.ch == nil {
		return nil
	}

	var errs []error

	if r.ch != nil {
		err := r.ch.Close()
		if err != nil {
			errs = append(errs, err)
		}
		r.ch = nil
	}

	if r.conn != nil {
		err := r.conn.Close()
		if err != nil {
			errs = append(errs, err)
		}
		r.conn = nil
	}

	var err error
	for i := range errs {
		err = fmt.Errorf("%w: %w", errs[i], err)
	}
	if err != nil {
		return fmt.Errorf("message queue close: %w", err)
	}

	return nil
}
