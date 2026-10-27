package main

import (
	"anonymizer/internal/clickhouse"
	"anonymizer/internal/config"
	"anonymizer/internal/consumer"
	"anonymizer/internal/pipeline"
	"anonymizer/internal/retry"

	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("program stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := config.GetConfig()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	kafkaConsumer := consumer.New(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID)
	defer kafkaConsumer.Close()

	clickhouseClient, err := clickhouse.New(ctx, clickhouse.Config{
		Addr:     cfg.ClickhouseAddr,
		Database: cfg.ClickhouseDatabase,
		Username: cfg.ClickhouseUsername,
		Password: cfg.ClickhousePassword,
		DialTimeout: cfg.ClickhouseDialTimeout,
		ReadTimeout: cfg.ClickhouseReadTimeout,
	})
	if err != nil {
		return err
	}
	defer clickhouseClient.Close()

	processor := pipeline.NewProcessor(slog.Default(), kafkaConsumer, clickhouseClient, cfg.LogBatchSize, cfg.LogBatchMaxTime, retry.DefaultBackoff)

	return processor.Start(ctx)
}
