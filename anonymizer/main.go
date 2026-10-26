package main

import (
	"anonymizer/internal/config"
	"anonymizer/internal/logic"
	"anonymizer/internal/consumer"
	"anonymizer/internal/clickhouse"

	"context"
	"log/slog"
	"golang.org/x/sync/errgroup"
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

	processor := logic.NewProcessor(slog.Default(), consumer, clickhouse, cfg.LogBatchSize, cfg.LogBatchMaxTime)

	g.Go(func() error {
		return logic.Fetch(ctx, consumer, messageChannel)
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
