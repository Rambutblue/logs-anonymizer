package main

import (
	"anonymizer/internal/config"
	"anonymizer/internal/logic"
	"anonymizer/internal/consumer"
	"anonymizer/internal/clickhouse"

	"github.com/segmentio/kafka-go"

	"context"
	"log"
)

func main() {
	cfg := config.GetConfig()
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.KafkaTopic,
		GroupID: cfg.KafkaGroupID,
	})
	defer r.Close()

	ctx := context.Background()
	channel := make(chan consumer.Message)
	consumer := consumer.New(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID)
	clickhouse, err := clickhouse.New(ctx, clickhouse.Config{
		Addr:     cfg.ClickhouseAddr,
		Database: cfg.ClickhouseDatabase,
		Username: cfg.ClickhouseUsername,
		Password: cfg.ClickhousePassword,
	})
	if err != nil {
		log.Fatal(err)
		return
	}
	defer clickhouse.Close()

	processor := logic.NewProcessor(consumer, clickhouse, cfg.LogBatchSize, cfg.LogBatchMaxTime)

	go logic.Fetch(ctx, consumer, channel)

	go processor.Run(ctx, channel)

	select {
	case <-ctx.Done():
		log.Println("Shutting down...")
		return
	}
}
