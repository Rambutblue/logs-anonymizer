package main

import (
	"anonymizer/internal/config"
	"anonymizer/internal/capnproto"
	"anonymizer/internal/logic"

	"github.com/segmentio/kafka-go"

	"context"
	"log"
)

func main() {
	cfg := config.GetConfig()
	// connect to kafka using cfg.KafkaBrokers, cfg.KafkaTopic, and cfg.KafkaGroupID
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.KafkaTopic,
		GroupID: cfg.KafkaGroupID,
	})
	defer r.Close()

	for {
		msg, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatalf("Error reading message: %v", err)
		}
		log.Printf("Received message: %v", msg)

		rec, err := capnproto.GetLog(&msg.Value)
		if err != nil {
			log.Printf("Error decoding message: %v", err)
			continue
		}
		rec = logic.Anonymize(rec)
		log.Printf(
			"Decoded: timestampEpochMilli=%d resourceId=%d bytesSent=%d requestTimeMilli=%d responseStatus=%d cacheStatus=%q method=%q remoteAddr=%q url=%q",
			rec.TimestampEpochMilli,
			rec.ResourceId,
			rec.BytesSent,
			rec.RequestTimeMilli,
			rec.ResponseStatus,
			rec.CacheStatus,
			rec.Method,
			rec.RemoteAddr,
			rec.URL,
		)
	}
}
