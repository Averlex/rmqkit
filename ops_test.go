package rmqkit

import (
	"context"
	"errors"
	"io"

	amqp "github.com/rabbitmq/amqp091-go"
)

type recordingAcknowledger struct {
	ack, nack         int
	tag               uint64
	multiple, requeue bool
	err               error
}

func (a *recordingAcknowledger) Ack(tag uint64, multiple bool) error {
	a.ack++
	a.tag, a.multiple = tag, multiple

	return a.err
}

func (a *recordingAcknowledger) Nack(tag uint64, multiple, requeue bool) error {
	a.nack++
	a.tag, a.multiple, a.requeue = tag, multiple, requeue

	return a.err
}

func (a *recordingAcknowledger) Reject(uint64, bool) error {
	panic("unexpected Reject")
}

type noOpCloser struct{}

func (noOpCloser) Close() error {
	return nil
}

func (s *ClientSuite) TestHandlerSettlement() {
	const deliveryTag = 7

	tests := []struct {
		name                     string
		failure, requeue, cancel bool
	}{
		{"successful", false, false, false},
		{"failed discard", true, false, false},
		{"failed requeue", true, true, false},
		{"cancelled during success", false, false, true},
		{"cancelled during failure", true, true, true},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			cfg := testConfig()
			cfg.RequeueOnError = tt.requeue
			r := s.newClient(cfg, FullClient)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ack := &recordingAcknowledger{}
			d := amqp.Delivery{
				Acknowledger:  ack,
				DeliveryTag:   deliveryTag,
				MessageId:     "application-id",
				Body:          []byte("payload"),
				CorrelationId: "correlation",
				Headers:       amqp.Table{"kind": "job"},
				Redelivered:   true,
				Exchange:      testExchange,
				RoutingKey:    testRoutingKey,
			}

			err := r.handleDelivery(ctx, noOpCloser{}, d, func(_ context.Context, got Delivery) error {
				s.Zero(ack.ack, "must acknowledge after processing")
				s.Zero(ack.nack, "must reject after processing")
				s.Equal(d.MessageId, got.MessageId)
				s.Equal(d.CorrelationId, got.CorrelationId)
				s.Equal(d.Headers, got.Headers)
				s.Equal(d.Body, got.Body)
				s.True(got.Redelivered)
				s.Equal(d.Exchange, got.Exchange)
				s.Equal(d.RoutingKey, got.RoutingKey)

				if tt.cancel {
					cancel()
				}

				if tt.failure {
					return errors.New("application failed")
				}

				return nil
			})

			if tt.cancel {
				s.ErrorIs(err, context.Canceled)
				s.Zero(ack.ack + ack.nack)

				return
			}

			s.Require().NoError(err)
			s.Equal(uint64(deliveryTag), ack.tag)
			s.False(ack.multiple)
			s.Equal(tt.requeue, ack.requeue)

			if tt.failure {
				s.Equal(1, ack.nack)
				s.Zero(ack.ack)
			} else {
				s.Equal(1, ack.ack)
				s.Zero(ack.nack)
			}
		})
	}
}

func (s *ClientSuite) TestSettlementFailurePreservesCause() {
	r := s.newClient(testConfig(), FullClient)
	ack := &recordingAcknowledger{err: io.EOF}

	err := r.handleDelivery(context.Background(), noOpCloser{}, amqp.Delivery{Acknowledger: ack},
		func(context.Context, Delivery) error { return nil })

	s.ErrorIs(err, io.EOF)
	s.Equal(1, ack.ack)
	s.Zero(ack.nack)
}

func (s *ClientSuite) TestPublicOperationGuards() {
	r := s.newClient(testConfig(), FullClient)
	ctx := context.Background()
	handler := func(context.Context, Delivery) error { return nil }

	s.ErrorIs(r.Produce(ctx, Message{}), ErrUninitialized)
	s.ErrorIs(r.Consume(ctx, nil), ErrInvalidHandler)
	s.ErrorIs(r.Consume(ctx, handler), ErrUninitialized)
	s.Require().NoError(r.Close(ctx))

	s.ErrorIs(r.Connect(ctx), ErrClosed)
	s.ErrorIs(r.Produce(ctx, Message{}), ErrClosed)

	producer := s.newClient(testConfig(), ProducerOnly)
	consumer := s.newClient(testConfig(), ConsumerOnly)

	s.ErrorIs(producer.Consume(ctx, handler), ErrClientType)
	s.ErrorIs(consumer.Produce(ctx, Message{}), ErrClientType)
}

func (s *ClientSuite) TestUnknownClientTypeIsRejected() {
	_, err := NewRabbitMQ(testConfig(), ConsumerOnly+1)

	s.ErrorIs(err, ErrInvalidConfig)
}
