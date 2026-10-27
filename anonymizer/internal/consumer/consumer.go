package consumer

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Message = kafka.Message

type Consumer struct {
	reader *kafka.Reader
}

func New(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
		}),
	}
}

func (c *Consumer) Fetch(ctx context.Context) (Message, error) {
	return c.reader.FetchMessage(ctx)
}

func (c *Consumer) Commit(ctx context.Context, msgs []Message) error {
	return c.reader.CommitMessages(ctx, msgs...)
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
