package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string

	LogBatchSize    int
	LogBatchMaxTime time.Duration
}

func GetConfig() Config {

	kafkaBrokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	kafkaTopic := getEnv("KAFKA_TOPIC", "logs")
	kafkaGroupID := getEnv("KAFKA_GROUP_ID", "logs-anonymizer")

	logBatchSize := getEnvInt("LOG_BATCH_SIZE", 100)
	logBatchMaxTime := time.Duration(getEnvInt("LOG_BATCH_MAX_TIME", 5)) * time.Second

	return Config{
		KafkaBrokers:    kafkaBrokers,
		KafkaTopic:      kafkaTopic,
		KafkaGroupID:    kafkaGroupID,
		LogBatchSize:    logBatchSize,
		LogBatchMaxTime: logBatchMaxTime,
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if intValue, err := strconv.Atoi(v); err == nil {
			return intValue
		}
	}
	return fallback
}
