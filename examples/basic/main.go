// Command basic publishes one application message and consumes jobs until interrupted.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Averlex/rmqkit"
)

const (
	shutdownTimeout   = 5 * time.Second
	exampleQueue      = "rmqkit-example-jobs"
	exampleExchange   = "rmqkit-example-events"
	exampleRoutingKey = "jobs.created"
	exampleMessageID  = "example-job"
)

func main() {
	if err := run(); err != nil {
		slog.Error("example failed", "error", err)
		os.Exit(1)
	}
}

func run() (result error) {
	cfg := rmqkit.DefaultConfig()
	cfg.Username, cfg.Password = os.Getenv("RMQ_USERNAME"), os.Getenv("RMQ_PASSWORD")
	cfg.Queue = exampleQueue
	cfg.Exchange = exampleExchange
	cfg.RoutingKey = exampleRoutingKey

	if host := os.Getenv("RMQ_HOST"); host != "" {
		cfg.Host = host
	}

	if port := os.Getenv("RMQ_PORT"); port != "" {
		parsed, err := strconv.Atoi(port)

		if err != nil {
			return fmt.Errorf("parse RMQ_PORT: %w", err)
		}

		cfg.Port = parsed
	}

	client, err := rmqkit.NewRabbitMQ(cfg, rmqkit.FullClient)

	if err != nil {
		return err
	}

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		result = errors.Join(result, client.Close(ctx))
	}()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		return err
	}

	jobs := rmqkit.Typed[Job](client)
	err = runJobs(ctx, jobs)

	if errors.Is(err, context.Canceled) {
		return nil
	}

	return err
}
