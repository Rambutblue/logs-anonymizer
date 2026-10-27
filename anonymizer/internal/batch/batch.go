package batch

import (
	"anonymizer/internal/consumer"
	"anonymizer/internal/model"
)

type Batch struct {
	messages []consumer.Message
	logs     []model.Log
}

func New() *Batch {
	return &Batch{
		messages: make([]consumer.Message, 0),
		logs:     make([]model.Log, 0),
	}
}

func (b *Batch) Add(msg consumer.Message, log model.Log) {
	b.messages = append(b.messages, msg)
	b.logs = append(b.logs, log)
}

func (b *Batch) Skip(msg consumer.Message) {
	b.messages = append(b.messages, msg)
}

func (b *Batch) Clear() {
	b.messages = b.messages[:0]
	b.logs = b.logs[:0]
}

func (b *Batch) Size() int {
	return len(b.messages)
}

func (b *Batch) LogsCount() int {
	return len(b.logs)
}

func (b *Batch) SkippedCount() int {
	return len(b.messages) - len(b.logs)
}

func (b *Batch) Logs() []model.Log {
	return b.logs
}

func (b *Batch) Messages() []consumer.Message {
	return b.messages
}
