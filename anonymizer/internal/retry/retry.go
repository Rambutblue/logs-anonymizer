package retry

import (
	"context"
	"log/slog"
	"time"
)

type Task func() error

const (
	maxRetryTime = 30 // in seconds
	maxAfter     = 5
)

func Retry(ctx context.Context, task Task, logger *slog.Logger) error {
	for attempts := 0; ; attempts++ {
		err := task()
		if err == nil {
			return nil
		}

		logger.Warn("task failed, retrying",
			"error", err,
			"attempts", attempts,
		)

		select {
		case <-time.After(calcTimeout(attempts, maxAfter)):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func calcTimeout(attempts int, max_after int) time.Duration {
	// Exponential with a max cap
	return time.Duration(min(2<<min(attempts, max_after), maxRetryTime)) * time.Second
}
