package logic

import (
	"anonymizer/internal/capnproto"
	"anonymizer/internal/clickhouse"
	"anonymizer/internal/consumer"
	"anonymizer/internal/httplog"
)

type Processor struct {
	consumer  Source
	clickhouse Inserter
}

func NewProcessor(consumer Source, clickhouse Inserter) *Processor {
	return &Processor{
		consumer:  consumer,
		clickhouse: clickhouse,
	}
}

func (p *Processor) ProcessAndInsert(channel <-chan consumer.Message) error {

}

func DecodeMessage(msg consumer.Message) (httplog.Log, error) {
	log, err := capnproto.GetLog(&msg.Value)
	if err != nil {
		return httplog.Log{}, err
	}
	return log, nil
}