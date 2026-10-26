package pipeline

import (
	"anonymizer/internal/anonymize"
	"anonymizer/internal/capnproto"
	"anonymizer/internal/consumer"
	"anonymizer/internal/model"
)

func ProcessMessage(msg *consumer.Message) (model.Log, error) {
	log, err := capnproto.GetLog(&msg.Value)
	if err != nil {
		return model.Log{}, err
	}

	// Add metadata
	log.KafkaTopic = msg.Topic
	log.KafkaPartition = uint16(msg.Partition)
	log.KafkaOffset = uint64(msg.Offset)

	return anonymize.Anonymize(log)
}
