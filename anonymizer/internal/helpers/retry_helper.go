package helpers

import(
	"context"
	"time"
	"log/slog"
)

type Task func () error

var (
	MAX_RETRIES_TIME = 30 * time.Second
	MAX_AFTER = 5
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
		case <-time.After(calcTimeout(attempts, MAX_AFTER)):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func calcTimeout(attempts int, max_after int) time.Duration {
	return time.Duration(max(attempts, max_after) / max_after) * MAX_RETRIES_TIME
}