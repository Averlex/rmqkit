package rmqkit

import (
	"crypto/tls"
	"encoding/json"
	"reflect"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (s *ClientSuite) TestConfigValidation() {
	tests := []struct {
		name   string
		change func(*Config)
	}{
		{"host", func(c *Config) { c.Host = " " }},
		{"port", func(c *Config) { c.Port = maxPort + 1 }},
		{"username", func(c *Config) { c.Username = "" }},
		{"password", func(c *Config) { c.Password = "" }},
		{"vhost", func(c *Config) { c.Vhost = "" }},
		{"connect timeout", func(c *Config) { c.ConnectTimeout = 0 }},
		{"operation timeout", func(c *Config) { c.OperationTimeout = -1 }},
		{"retry delay", func(c *Config) { c.RetryDelay = 0 }},
		{"retries", func(c *Config) { c.Retries = -1 }},
		{"queue", func(c *Config) { c.Queue = "" }},
		{"reserved queue", func(c *Config) { c.Queue = reservedNamePrefix + testQueue }},
		{"reserved exchange", func(c *Config) { c.Exchange = reservedNamePrefix + testExchange }},
		{"routing key", func(c *Config) { c.RoutingKey = strings.Repeat("x", maxShortStringLength+1) }},
		{"default exchange binding", func(c *Config) { c.Exchange = amqp.DefaultExchange }},
		{"exchange type", func(c *Config) { c.ExchangeType = "invalid" }},
		{"prefetch", func(c *Config) { c.Prefetch = minPrefetch - 1 }},
		{"TLS mismatch", func(c *Config) { c.TLSConfig = &tls.Config{} }},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			cfg := testConfig()
			tt.change(&cfg)

			_, err := NewRabbitMQ(cfg, FullClient)
			s.Require().ErrorIs(err, ErrInvalidConfig)

			if cfg.Password != "" {
				s.NotContains(err.Error(), cfg.Password)
			}
		})
	}
}

func (s *ClientSuite) TestSupportedExchangeTypes() {
	for _, exchangeType := range []string{amqp.ExchangeDirect, amqp.ExchangeTopic, amqp.ExchangeFanout} {
		s.Run(exchangeType, func() {
			cfg := testConfig()
			cfg.ExchangeType = exchangeType

			s.NoError(cfg.Validate())
		})
	}

	s.Run("default exchange", func() {
		cfg := testConfig()
		cfg.Exchange = amqp.DefaultExchange
		cfg.RoutingKey = cfg.Queue

		s.NoError(cfg.Validate())
	})
}

func (s *ClientSuite) TestConfigurationTagsAndJSON() {
	typ := reflect.TypeFor[Config]()

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		for _, tag := range []string{"json", "yaml", "env"} {
			s.NotEmpty(field.Tag.Get(tag), "%s lacks %s tag", field.Name, tag)
		}
	}

	cfg := DefaultConfig()
	err := json.Unmarshal([]byte(`{"username":"user","password":"secret","queue":"jobs","routing_key":"jobs","operation_timeout":2000000000}`), &cfg)
	s.Require().NoError(err)
	s.Require().NoError(cfg.Validate())

	s.Equal(2*time.Second, cfg.OperationTimeout)
	s.Equal(DefaultPrefetch, cfg.Prefetch)
}

func (s *ClientSuite) TestTLSConfigurationIsCloned() {
	const originalName = "original.example"

	cfg := testConfig()
	cfg.TLS = true
	cfg.TLSConfig = &tls.Config{ServerName: originalName, MinVersion: tls.VersionTLS12}
	r := s.newClient(cfg, FullClient)

	cfg.TLSConfig.ServerName = "changed.example"

	s.Equal(originalName, r.cfg.TLSConfig.ServerName)
}
