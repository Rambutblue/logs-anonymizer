package main

import (
	"anonymizer/internal/clickhouse"
	"anonymizer/internal/config"
	"anonymizer/internal/consumer"
	"anonymizer/internal/pipeline"

	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("program stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := config.GetConfig()
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	g, ctx := errgroup.WithContext(signalCtx)

	messageChannel := make(chan consumer.Message)

	kafkaConsumer := consumer.New(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID)
	defer kafkaConsumer.Close()

	clickhouseClient, err := clickhouse.New(ctx, clickhouse.Config{
		Addr:     cfg.ClickhouseAddr,
		Database: cfg.ClickhouseDatabase,
		Username: cfg.ClickhouseUsername,
		Password: cfg.ClickhousePassword,
	})
	if err != nil {
		return err
	}
	defer clickhouseClient.Close()

	processor := pipeline.NewProcessor(slog.Default(), kafkaConsumer, clickhouseClient, cfg.LogBatchSize, cfg.LogBatchMaxTime)

	g.Go(func() error {
		return pipeline.Fetch(ctx, kafkaConsumer, messageChannel)
	})

	g.Go(func() error {
		return processor.Run(ctx, messageChannel)
	})

	return g.Wait()
}
