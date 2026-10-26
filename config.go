package rmqkit

import (
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Config describes one queue and its binding. Start with DefaultConfig before
// decoding settings. File and environment loading belongs to the caller.
// Duration units depend on the decoder; encoding/json uses integer nanoseconds.
type Config struct {
	Host     string `json:"host" yaml:"host" env:"RMQ_HOST"`
	Port     int    `json:"port" yaml:"port" env:"RMQ_PORT"`
	Username string `json:"username" yaml:"username" env:"RMQ_USERNAME"`
	Password string `json:"password" yaml:"password" env:"RMQ_PASSWORD"`
	Vhost    string `json:"vhost" yaml:"vhost" env:"RMQ_VHOST"`
	TLS      bool   `json:"tls" yaml:"tls" env:"RMQ_TLS"`

	ConnectTimeout   time.Duration `json:"connect_timeout" yaml:"connect_timeout" env:"RMQ_CONNECT_TIMEOUT"`
	OperationTimeout time.Duration `json:"operation_timeout" yaml:"operation_timeout" env:"RMQ_OPERATION_TIMEOUT"`
	RetryDelay       time.Duration `json:"retry_delay" yaml:"retry_delay" env:"RMQ_RETRY_DELAY"`
	Retries          int           `json:"retries" yaml:"retries" env:"RMQ_RETRIES"`

	Queue        string `json:"queue" yaml:"queue" env:"RMQ_QUEUE"`
	Exchange     string `json:"exchange" yaml:"exchange" env:"RMQ_EXCHANGE"`
	ExchangeType string `json:"exchange_type" yaml:"exchange_type" env:"RMQ_EXCHANGE_TYPE"`
	RoutingKey   string `json:"routing_key" yaml:"routing_key" env:"RMQ_ROUTING_KEY"`
	Durable      bool   `json:"durable" yaml:"durable" env:"RMQ_DURABLE"`

	Persistent     bool   `json:"persistent" yaml:"persistent" env:"RMQ_PERSISTENT"`
	ContentType    string `json:"content_type" yaml:"content_type" env:"RMQ_CONTENT_TYPE"`
	Prefetch       int    `json:"prefetch" yaml:"prefetch" env:"RMQ_PREFETCH"`
	RequeueOnError bool   `json:"requeue_on_error" yaml:"requeue_on_error" env:"RMQ_REQUEUE_ON_ERROR"`

	// TLSConfig optionally supplies custom trust roots or client certificates.
	// It requires TLS=true and is cloned at construction. Certificate pools and
	// callback state must not be mutated while the client is in use.
	TLSConfig *tls.Config `json:"-" yaml:"-" env:"-"`
}

// DefaultConfig returns transport defaults without credentials or topology names.
// Port=0 selects 5672, or 5671 when TLS is enabled.
func DefaultConfig() Config {
	return Config{
		Host:             DefaultHost,
		Vhost:            DefaultVhost,
		ConnectTimeout:   DefaultConnectTimeout,
		OperationTimeout: DefaultOperationTimeout,
		RetryDelay:       DefaultRetryDelay,
		Retries:          DefaultRetries,
		ExchangeType:     DefaultExchangeType,
		Durable:          DefaultDurable,
		Persistent:       DefaultPersistent,
		ContentType:      DefaultContentType,
		Prefetch:         DefaultPrefetch,
	}
}

// Validate checks configuration without opening a connection. Empty Exchange
// selects the default exchange, whose routing key must equal Queue.
func (c Config) Validate() error {
	var reason string

	switch {
	case strings.TrimSpace(c.Host) == "":
		reason = "host is required"
	case c.Port < 0 || c.Port > maxPort:
		reason = fmt.Sprintf("port must be between 0 and %d", maxPort)
	case c.Username == "" || c.Password == "":
		reason = "username and password are required"
	case c.Vhost == "":
		reason = "vhost is required"
	case c.ConnectTimeout <= 0 || c.OperationTimeout <= 0 || c.RetryDelay <= 0:
		reason = "timeouts and retry delay must be positive"
	case c.Retries < 0:
		reason = "retries must not be negative"
	case c.Queue == "" || len(c.Queue) > maxShortStringLength || strings.HasPrefix(c.Queue, reservedNamePrefix):
		reason = fmt.Sprintf("queue must be a non-reserved name of at most %d bytes", maxShortStringLength)
	case len(c.Exchange) > maxShortStringLength || strings.HasPrefix(c.Exchange, reservedNamePrefix):
		reason = fmt.Sprintf("exchange must be a non-reserved name of at most %d bytes", maxShortStringLength)
	case len(c.RoutingKey) > maxShortStringLength:
		reason = fmt.Sprintf("routing key must be at most %d bytes", maxShortStringLength)
	case c.Exchange == "" && c.RoutingKey != c.Queue:
		reason = "default exchange requires routing_key to equal queue"
	case c.Exchange != "" && c.ExchangeType != amqp.ExchangeDirect && c.ExchangeType != amqp.ExchangeTopic && c.ExchangeType != amqp.ExchangeFanout:
		reason = "exchange type must be direct, topic, or fanout"
	case c.Prefetch < minPrefetch || c.Prefetch > maxPrefetch:
		reason = fmt.Sprintf("prefetch must be between %d and %d", minPrefetch, maxPrefetch)
	case c.TLSConfig != nil && !c.TLS:
		reason = "TLSConfig requires TLS"
	}

	if reason != "" {
		return fmt.Errorf("%w: %s", ErrInvalidConfig, reason)
	}

	return nil
}
