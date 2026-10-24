package main

import (
	"anonymizer/internal/config"

	"capnproto.org/go/capnp/v3"
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

		// decode the message from capnproto
		cm, err := capnp.Unmarshal(msg.Value)
		if err != nil {
			log.Fatalf("Error unmarshalling capnp message: %v", err)
		}
		root, err := cm.Root()
		if err != nil {
			log.Fatalf("Error reading capnp root: %v", err)
		}
		s := root.Struct()
		text := func(i uint16) string {
			p, err := s.Ptr(i)
			if err != nil {
				log.Fatalf("Error reading pointer %d: %v", i, err)
			}
			return p.Text()
		}
		log.Printf(
			"Decoded: timestampEpochMilli=%d resourceId=%d bytesSent=%d requestTimeMilli=%d responseStatus=%d cacheStatus=%q method=%q remoteAddr=%q url=%q",
			s.Uint64(0), s.Uint64(8), s.Uint64(16), s.Uint64(24), s.Uint16(32),
			text(0), text(1), text(2), text(3),
		)
	}
}
