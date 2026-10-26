package logic

import (
	"context"
	"anonymizer/internal/consumer"
)

type Source interface {
    Fetch(ctx context.Context) (consumer.Message, error)
    Commit(ctx context.Context, msgs ...consumer.Message) error
}

type Inserter interface {
	InsertBatch(ctx context.Context, logs []consumer.Message) error
}
