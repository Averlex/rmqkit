package rmqkit

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

// A stalled local peer exercises handshake cleanup without a running broker.
func (s *ClientSuite) stalledPeer() (string, int, <-chan struct{}, <-chan struct{}) {
	s.T().Helper()

	listener, err := net.Listen(networkTCP, "127.0.0.1:0")
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = listener.Close() })

	host, portText, err := net.SplitHostPort(listener.Addr().String())
	s.Require().NoError(err)

	port, err := strconv.Atoi(portText)
	s.Require().NoError(err)

	accepted, ended := make(chan struct{}), make(chan struct{})

	go func() {
		defer close(ended)

		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()

		_ = conn.SetDeadline(time.Now().Add(testTimeout))
		close(accepted)

		_, _ = io.Copy(io.Discard, conn)
	}()

	return host, port, accepted, ended
}

func (s *ClientSuite) TestConnectCancellationAndCleanup() {
	for _, mode := range []string{"caller", "close", "timeout"} {
		s.Run(mode, func() {
			host, port, accepted, ended := s.stalledPeer()
			cfg := testConfig()
			cfg.Host, cfg.Port = host, port
			cfg.Retries = 0

			if mode == "timeout" {
				cfg.ConnectTimeout = testHandshakeTimeout
			}

			r := s.newClient(cfg, FullClient)
			ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
			defer cancel()

			result := make(chan error, 1)
			go func() { result <- r.Connect(ctx) }()

			select {
			case <-accepted:
			case <-ctx.Done():
				s.Require().NoError(ctx.Err(), "peer never accepted connection")
			}

			switch mode {
			case "caller":
				cancel()
			case "close":
				s.Require().NoError(r.Close(ctx))
			}

			select {
			case err := <-result:
				want := context.Canceled

				if mode == "timeout" {
					want = context.DeadlineExceeded
					s.ErrorIs(err, ErrTimeout)
				}

				s.ErrorIs(err, want)
			case <-time.After(testTimeout):
				s.FailNow("Connect did not stop")
			}

			select {
			case <-ended:
			case <-time.After(testTimeout):
				s.FailNow("socket was not closed")
			}

			r.mu.Lock()
			defer r.mu.Unlock()

			s.Empty(r.sockets)
			s.Zero(r.active)
			s.False(r.connected)
		})
	}
}

func (s *ClientSuite) TestCloseWaitsForWorkAndCanBeRetried() {
	r := s.newClient(testConfig(), FullClient)
	workCtx, done, err := r.begin(context.Background(), false)
	s.Require().NoError(err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s.ErrorIs(r.Close(ctx), context.Canceled)

	select {
	case <-workCtx.Done():
	case <-time.After(testTimeout):
		s.FailNow("work was not cancelled")
	}

	done()

	s.NoError(r.Close(context.Background()))
}

func (s *ClientSuite) TestConcurrentCloseAndOperations() {
	const workers = 20

	r := s.newClient(testConfig(), FullClient)
	results := make(chan error, workers)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
			defer cancel()

			_ = r.Produce(ctx, Message{})
			results <- r.Close(ctx)
		}()
	}

	wg.Wait()
	close(results)

	for err := range results {
		s.NoError(err)
	}
}
