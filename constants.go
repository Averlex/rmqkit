package rmqkit

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// FullClient declares topology and supports publishing and consuming.
	FullClient ClientType = iota
	// ProducerOnly declares topology and supports publishing.
	ProducerOnly
	// ConsumerOnly requires an existing queue and supports consuming.
	ConsumerOnly
)

// Default transport and topology settings used by DefaultConfig.
const (
	DefaultHost = "localhost"
	// DefaultPort and DefaultTLSPort are resolved when Config.Port is zero.
	DefaultPort             = 5672
	DefaultTLSPort          = 5671
	DefaultVhost            = "/"
	DefaultConnectTimeout   = 10 * time.Second
	DefaultOperationTimeout = 10 * time.Second
	DefaultRetryDelay       = time.Second
	DefaultRetries          = 3
	DefaultExchangeType     = amqp.ExchangeDirect
	DefaultDurable          = true
	DefaultPersistent       = true
	DefaultContentType      = "application/json"
	DefaultPrefetch         = 1
)

const (
	maxPort              = 65535
	maxShortStringLength = 255
	reservedNamePrefix   = "amq."
	minPrefetch          = 1
	maxPrefetch          = 65535
	heartbeatInterval    = 10 * time.Second
	amqpLocale           = "en_US"
	networkTCP           = "tcp"
	publisherConcurrency = 1
	notificationBuffer   = 1
)
