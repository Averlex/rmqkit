package rmqkit

// File contains declarations for expected configuration fields based on the client type.

import "time"

// expectedFields is a map of expected configuration fields and their default values.
var expectedFieldsFull = map[string]any{
	"host":     "",
	"port":     "",
	"user":     "",
	"password": "",

	"timeout":       time.Duration(0),
	"retry_timeout": time.Duration(0),
	"retries":       0,

	"topic":        "",
	"durable":      false,
	"content_type": "",

	"routing_key": "",

	"resub_timeout": time.Duration(0), // Resubscription timeout for the consumer.
	"auto_ack":      false,
	"requeue":       false,
}

var expectedFieldsConsumer = map[string]any{
	"host":     "",
	"port":     "",
	"user":     "",
	"password": "",

	"timeout":       time.Duration(0),
	"retry_timeout": time.Duration(0),
	"retries":       0,

	"topic":   "",
	"durable": false,

	"resub_timeout": time.Duration(0), // Resubscription timeout for the consumer.
	"auto_ack":      false,
	"requeue":       false,
}

var expectedFieldsProducer = map[string]any{
	"host":     "",
	"port":     "",
	"user":     "",
	"password": "",

	"timeout":       time.Duration(0),
	"retry_timeout": time.Duration(0),
	"retries":       0,

	"topic":        "",
	"durable":      false,
	"content_type": "",

	"routing_key": "",
}
