package logic

import (
	"anonymizer/internal/consumer"
	"context"
)

func Fetch(ctx context.Context, source Source, channel chan<- consumer.Message) error {
	defer close(channel)
	for {
		msg, err := source.Fetch(ctx)
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
