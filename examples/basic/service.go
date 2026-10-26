package main

import (
	"context"
	"log/slog"
)

// Job is application data; it has no required transport envelope.
type Job struct {
	ID   string `json:"id"`
	Task string `json:"task"`
}

// JobBroker is owned by the application layer and uses only application types.
type JobBroker interface {
	Publish(context.Context, Job) error
	Subscribe(context.Context, func(context.Context, Job) error) error
}

func runJobs(ctx context.Context, broker JobBroker) error {
	if err := broker.Publish(ctx, Job{ID: exampleMessageID, Task: "demo"}); err != nil {
		return err
	}

	return broker.Subscribe(ctx, func(ctx context.Context, job Job) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		slog.InfoContext(ctx, "processed job", "job_id", job.ID)

		return nil
	})
}
