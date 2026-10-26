package rmqkit

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const (
	testTimeout          = 5 * time.Second
	testHandshakeTimeout = 50 * time.Millisecond
	testUsername         = "test-user"
	testPassword         = "test-password"
	testQueue            = "jobs"
	testExchange         = "events"
	testRoutingKey       = "jobs.created"
)

type ClientSuite struct {
	suite.Suite
}

func TestClientSuite(t *testing.T) {
	suite.Run(t, new(ClientSuite))
}

func testConfig() Config {
	cfg := DefaultConfig()
	cfg.Username = testUsername
	cfg.Password = testPassword
	cfg.Queue = testQueue
	cfg.Exchange = testExchange
	cfg.RoutingKey = testRoutingKey

	return cfg
}

func (s *ClientSuite) newClient(cfg Config, typ ClientType) *RabbitMQ {
	s.T().Helper()
	t := s.T()

	r, err := NewRabbitMQ(cfg, typ)
	s.Require().NoError(err)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()

		assert.NoError(t, r.Close(ctx))
	})

	return r
}
