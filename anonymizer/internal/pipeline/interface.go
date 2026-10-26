package pipeline

import (
	"anonymizer/internal/consumer"
	"anonymizer/internal/model"
	"context"
)

type Source interface {
	Fetch(ctx context.Context) (consumer.Message, error)
	Commit(ctx context.Context, msgs []consumer.Message) error
}

type Inserter interface {
	InsertBatch(ctx context.Context, logs []model.Log) error
}
