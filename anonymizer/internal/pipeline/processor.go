package pipeline

import (
	"anonymizer/internal/batch"
	"anonymizer/internal/consumer"
	"anonymizer/internal/retry"
	"context"
	"time"

	"log/slog"
)

type Processor struct {
	logger     *slog.Logger
	consumer   Source
	clickhouse Inserter
	sizeLimit  int
	timeLimit  time.Duration
}

func NewProcessor(logger *slog.Logger, consumer Source, clickhouse Inserter, sizeLimit int, timeLimit time.Duration) *Processor {
	return &Processor{
		logger:     logger,
		consumer:   consumer,
		clickhouse: clickhouse,
		sizeLimit:  sizeLimit,
		timeLimit:  timeLimit,
	}
}

func (p *Processor) Run(ctx context.Context, channel <-chan consumer.Message) error {
	batch := batch.NewBatch()
	var flushTimer <-chan time.Time

	for {
		select {
		//TODO: Check time limit
		case msg, ok := <-channel:
			if !ok {
				// Channel closed, flush remaining messages
				return p.InsertBatch(context.WithoutCancel(ctx), batch)
			}
			log, err := ProcessMessage(&msg)
			if err != nil {
				p.logger.Warn("dropping message",
					"partition", msg.Partition, "offset", msg.Offset,
					"error", err)

				batch.Skip(msg)
			} else {
				batch.Add(msg, log)
			}

			if batch.Size() == 1 {
				flushTimer = time.After(p.timeLimit)
			}

			if batch.Size() >= p.sizeLimit {
				err = p.InsertBatch(ctx, batch)
				if err != nil {
					return err
				}
				flushTimer = nil
			}

		case <-flushTimer:
			err := p.InsertBatch(ctx, batch)
			if err != nil {
				return err
			}
			flushTimer = nil
		}
	}
}

func (p *Processor) InsertBatch(ctx context.Context, batch *batch.Batch) error {
	err := retry.Retry(ctx, func() error {
		return p.clickhouse.InsertBatch(ctx, batch.Logs())
	}, p.logger)
	if err != nil {
		return err
	}

	err = p.consumer.Commit(ctx, batch.Messages())
	if err != nil {
		p.logger.Warn("failed to commit processed messages",
			"partition", batch.Messages()[0].Partition, "offset", batch.Messages()[0].Offset,
			"error", err)
	}

	p.logger.Info("batch inserted and committed",
		"inserted", batch.LogsCount(),
		"skipped", batch.SkippedCount(),
	)

	batch.Clear()

	return nil
}
