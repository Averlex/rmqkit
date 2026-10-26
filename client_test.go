package rmqkit

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type testEvent struct {
	ID    string `json:"id"`
	Count int    `json:"count"`
}

type eventBroker interface {
	Publish(context.Context, testEvent) error
	Subscribe(context.Context, func(context.Context, testEvent) error) error
}

var _ eventBroker = (*Client[testEvent])(nil)

type stubMessageTransport struct {
	produce func(context.Context, Message) error
	consume func(context.Context, Handler) error
}

func (t *stubMessageTransport) Produce(ctx context.Context, msg Message) error {
	return t.produce(ctx, msg)
}

func (t *stubMessageTransport) Consume(ctx context.Context, handler Handler) error {
	return t.consume(ctx, handler)
}

func (s *ClientSuite) TestTypedPublishEncodesWholeValue() {
	ctx := context.Background()
	message := testEvent{ID: "application-id", Count: 3}
	called := false
	transport := &stubMessageTransport{
		produce: func(gotCtx context.Context, got Message) error {
			called = true

			s.Equal(ctx, gotCtx)
			s.JSONEq(`{"id":"application-id","count":3}`, string(got.Body))
			s.Equal(DefaultContentType, got.ContentType)
			s.Empty(got.MessageId, "application identifiers must not be interpreted")
			s.Empty(got.Headers, "application fields must stay in the body")

			return ErrPublishUncertain
		},
	}
	client := &Client[testEvent]{transport: transport}

	err := client.Publish(ctx, message)

	s.True(called)
	s.ErrorIs(err, ErrPublishUncertain)
}

func (s *ClientSuite) TestTypedPublishJSONShapes() {
	tests := []struct {
		name  string
		value any
		json  string
	}{
		{"map", map[string]int{"count": 3}, `{"count":3}`},
		{"slice", []int{1, 2}, `[1,2]`},
		{"pointer", &testEvent{ID: "job", Count: 1}, `{"id":"job","count":1}`},
		{"nil pointer", (*testEvent)(nil), `null`},
		{"bytes follow JSON rules", []byte("job"), `"am9i"`},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			transport := &stubMessageTransport{
				produce: func(_ context.Context, msg Message) error {
					s.JSONEq(tt.json, string(msg.Body))

					return nil
				},
			}
			client := &Client[any]{transport: transport}

			s.NoError(client.Publish(context.Background(), tt.value))
		})
	}
}

func (s *ClientSuite) TestTypedPublishRejectsUnsupportedJSON() {
	called := false
	transport := &stubMessageTransport{
		produce: func(context.Context, Message) error {
			called = true

			return nil
		},
	}
	client := &Client[chan int]{transport: transport}

	err := client.Publish(context.Background(), make(chan int))

	var cause *json.UnsupportedTypeError
	s.ErrorIs(err, ErrInvalidMessage)
	s.ErrorAs(err, &cause)
	s.False(called, "serialization must fail before transport I/O")
}

func (s *ClientSuite) TestTypedSubscribePreservesContextAndHandlerError() {
	ctx := context.Background()
	handlerFailure := errors.New("application processing failed")
	called := false
	transport := &stubMessageTransport{
		consume: func(gotCtx context.Context, handler Handler) error {
			s.Equal(ctx, gotCtx)

			err := handler(ctx, Delivery{Message: Message{Body: []byte(`{"id":"job","count":3}`)}})
			s.ErrorIs(err, handlerFailure)

			return ErrRetriesExceeded
		},
	}
	client := &Client[testEvent]{transport: transport}

	err := client.Subscribe(ctx, func(gotCtx context.Context, message testEvent) error {
		called = true

		s.Equal(ctx, gotCtx)
		s.Equal(testEvent{ID: "job", Count: 3}, message)

		return handlerFailure
	})

	s.True(called)
	s.ErrorIs(err, ErrRetriesExceeded)
}

func (s *ClientSuite) TestTypedSubscribeSettlement() {
	tests := []struct {
		name         string
		body         string
		handlerError error
		requeue      bool
		cancel       bool
	}{
		{"success", `{"id":"job"}`, nil, false, false},
		{"handler failure", `{"id":"job"}`, io.EOF, false, false},
		{"handler requeue", `{"id":"job"}`, io.EOF, true, false},
		{"malformed JSON", `{`, nil, false, false},
		{"wrong JSON type", `{"count":"invalid"}`, nil, true, false},
		{"cancelled processing", `{"id":"job"}`, nil, false, true},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			cfg := testConfig()
			cfg.RequeueOnError = tt.requeue
			r := s.newClient(cfg, FullClient)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ack := &recordingAcknowledger{}
			transport := &stubMessageTransport{
				consume: func(ctx context.Context, handler Handler) error {
					return r.handleDelivery(ctx, noOpCloser{}, amqp.Delivery{
						Acknowledger: ack,
						Body:         []byte(tt.body),
					}, handler)
				},
			}
			client := &Client[testEvent]{transport: transport}
			called := false

			err := client.Subscribe(ctx, func(_ context.Context, message testEvent) error {
				called = true

				s.Zero(ack.ack+ack.nack, "processing must finish before settlement")
				s.Equal("job", message.ID)

				if tt.cancel {
					cancel()
				}

				return tt.handlerError
			})

			if tt.cancel {
				s.ErrorIs(err, context.Canceled)
				s.Zero(ack.ack + ack.nack)

				return
			}

			s.Require().NoError(err)

			if tt.handlerError != nil || !called {
				s.Equal(1, ack.nack)
				s.Zero(ack.ack)
				s.Equal(tt.requeue, ack.requeue)
			} else {
				s.Equal(1, ack.ack)
				s.Zero(ack.nack)
			}

			if tt.name == "malformed JSON" || tt.name == "wrong JSON type" {
				s.False(called, "invalid messages must not reach the application handler")
			}
		})
	}
}

func (s *ClientSuite) TestTypedSubscribeDecodeFailurePreservesCause() {
	transport := &stubMessageTransport{
		consume: func(ctx context.Context, handler Handler) error {
			return handler(ctx, Delivery{Message: Message{Body: []byte("{")}})
		},
	}
	client := &Client[testEvent]{transport: transport}
	called := false

	err := client.Subscribe(context.Background(), func(context.Context, testEvent) error {
		called = true

		return nil
	})

	var cause *json.SyntaxError
	s.ErrorIs(err, ErrInvalidMessage)
	s.ErrorAs(err, &cause)
	s.False(called)
}

func (s *ClientSuite) TestTypedClientsShareTransportLifecycle() {
	transport := s.newClient(testConfig(), FullClient)
	events := Typed[testEvent](transport)
	other := Typed[string](transport)
	ctx := context.Background()

	s.ErrorIs(events.Publish(ctx, testEvent{}), ErrUninitialized)
	s.ErrorIs(other.Publish(ctx, "another type"), ErrUninitialized)
	s.Require().NoError(transport.Close(ctx))

	s.ErrorIs(events.Publish(ctx, testEvent{}), ErrClosed)
	s.ErrorIs(other.Subscribe(ctx, func(context.Context, string) error { return nil }), ErrClosed)
}

func (s *ClientSuite) TestTypedClientGuards() {
	tests := []struct {
		name   string
		client *Client[testEvent]
	}{
		{"nil wrapper", nil},
		{"zero wrapper", &Client[testEvent]{}},
		{"nil transport", Typed[testEvent](nil)},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.ErrorIs(tt.client.Publish(context.Background(), testEvent{}), ErrUninitialized)
			s.ErrorIs(tt.client.Subscribe(context.Background(), func(context.Context, testEvent) error { return nil }), ErrUninitialized)
		})
	}

	client := Typed[testEvent](s.newClient(testConfig(), FullClient))
	s.ErrorIs(client.Subscribe(context.Background(), nil), ErrInvalidHandler)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s.ErrorIs(client.Publish(ctx, testEvent{}), context.Canceled)
	s.ErrorIs(client.Subscribe(ctx, func(context.Context, testEvent) error { return nil }), context.Canceled)
}

func (s *ClientSuite) TestTypedPublishConcurrentCalls() {
	const workers = 20

	var mu sync.Mutex
	received := make(map[int]bool)
	transport := &stubMessageTransport{
		produce: func(_ context.Context, msg Message) error {
			var id int

			if err := json.Unmarshal(msg.Body, &id); err != nil {
				return err
			}

			mu.Lock()
			received[id] = true
			mu.Unlock()

			return nil
		},
	}
	client := &Client[int]{transport: transport}
	results := make(chan error, workers)
	var wg sync.WaitGroup

	for id := 0; id < workers; id++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			results <- client.Publish(context.Background(), id)
		}(id)
	}

	wg.Wait()
	close(results)

	for err := range results {
		s.NoError(err)
	}

	s.Len(received, workers)
}
