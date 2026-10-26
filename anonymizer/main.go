package main

import (
	"anonymizer/internal/clickhouse"
	"anonymizer/internal/config"
	"anonymizer/internal/consumer"
	"anonymizer/internal/pipeline"

	"context"
	"golang.org/x/sync/errgroup"
	"log/slog"
)

func main() {
	cfg := config.GetConfig()
	bgCtx := context.Background()
	g, ctx := errgroup.WithContext(bgCtx)

	messageChannel := make(chan consumer.Message)

	consumer := consumer.New(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID)
	defer consumer.Close()

	clickhouse, err := clickhouse.New(ctx, clickhouse.Config{
		Addr:     cfg.ClickhouseAddr,
		Database: cfg.ClickhouseDatabase,
		Username: cfg.ClickhouseUsername,
		Password: cfg.ClickhousePassword,
	})
	if err != nil {
		slog.Error("failed to create clickhouse client", "error", err)
		return
	}
	defer clickhouse.Close()

	processor := pipeline.NewProcessor(slog.Default(), consumer, clickhouse, cfg.LogBatchSize, cfg.LogBatchMaxTime)

	g.Go(func() error {
		return pipeline.Fetch(ctx, consumer, messageChannel)
	})

	g.Go(func() error {
		return processor.Run(ctx, messageChannel)
	})

	err = g.Wait()

	if err != nil {
		slog.Error("Error occurred", "error", err)
	}

	slog.Info("Shutting down...")
}
