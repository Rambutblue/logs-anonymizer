package logic

import (
	"anonymizer/internal/consumer"
	"context"
)

func Fetch(ctx context.Context, source Source, channel chan<- consumer.Message) error {
	select {
	case <-ctx.Done():
		return nil
	default:
		msg, err := source.Fetch(ctx)
		if err != nil {
			return err
		}
		channel <- msg
		return nil
	}
}