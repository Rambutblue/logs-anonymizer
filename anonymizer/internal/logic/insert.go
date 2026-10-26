package logic

import (
	"anonymizer/internal/capnproto"
	"anonymizer/internal/consumer"
	"anonymizer/internal/httplog"
	"context"
	"time"
)

type Processor struct {
	consumer  Source
	clickhouse Inserter
	sizeLimit int
	timeLimit time.Duration
}

func NewProcessor(consumer Source, clickhouse Inserter, sizeLimit int, timeLimit time.Duration) *Processor {
	return &Processor{
		consumer:  consumer,
		clickhouse: clickhouse,
		sizeLimit:  sizeLimit,
		timeLimit:  timeLimit,
	}
}

func (p *Processor) Run(ctx context.Context, channel <-chan consumer.Message) error {
	batch := NewBatch()

	select {
	case <-ctx.Done():
		return nil
	//TODO: Check time limit
	case msg := <-channel:
		log, err := ProcessMessage(&msg.Value)
		if err != nil {
			//TODO: Log the error and skip the message
			batch.Skip(msg)
		} else {
			batch.Add(msg, log)
		}

		if (batch.Size() >= p.sizeLimit) {
			//TODO: Handle the errors
			err = p.clickhouse.InsertBatch(ctx, batch.Logs())
			err = p.consumer.Commit(ctx, batch.Messages()...)
			batch.Clear()
		}
	}

	return nil
}

func ProcessMessage(msg *[]byte) (httplog.Log, error) {
	log, err := capnproto.GetLog(msg)
	if err != nil {
		return httplog.Log{}, err
	}
	
	return Anonymize(log), nil
}