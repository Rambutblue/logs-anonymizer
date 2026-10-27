package pipeline

import (
	"anonymizer/internal/consumer"
	"anonymizer/internal/retry"
	"context"
	"log/slog"
)

type Fetcher struct {
	logger  *slog.Logger
	backoff retry.Backoff
	source  Source
}

func NewFetcher(logger *slog.Logger, backoff retry.Backoff, source Source) *Fetcher {
	return &Fetcher{
		logger:  logger,
		backoff: backoff,
		source:  source,
	}
}

func (f *Fetcher) Fetch(ctx context.Context, channel chan<- consumer.Message) error {
	defer close(channel)
	for {
		var msg consumer.Message
		err := retry.Retry(ctx, func() error {
			m, err := f.source.Fetch(ctx)
			if err != nil {
				return err
			}
			msg = m
			return nil
		}, f.backoff, f.logger)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		select {
		case channel <- msg:
		case <-ctx.Done():
			return nil
		}
	}
}
