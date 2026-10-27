package clickhouse

import (
	"anonymizer/internal/model"

	"context"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Config struct {
	Addr        string
	Database    string
	Username    string
	Password    string
	DialTimeout time.Duration
	ReadTimeout time.Duration
}

type Client struct {
	connection driver.Conn
}

func New(ctx context.Context, cfg Config) (*Client, error) {
	conn, err := ch.Open(&ch.Options{
		Addr: []string{cfg.Addr},
		Auth: ch.Auth{Database: cfg.Database, Username: cfg.Username, Password: cfg.Password},
		DialTimeout: cfg.DialTimeout,
		ReadTimeout: cfg.ReadTimeout,
		Compression: &ch.Compression{Method: ch.CompressionLZ4},
	})
	if err != nil {
		return nil, err
	}

	err = conn.Ping(ctx)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &Client{
		connection: conn,
	}, nil

}

func (c *Client) InsertBatch(ctx context.Context, logs []model.Log) error {
	if len(logs) == 0 {
		return nil
	}

	batchCtx := ch.Context(ctx, ch.WithSettings(ch.Settings{"insert_deduplicate": 1, "deduplicate_blocks_in_dependent_materialized_views": 1}))
	batch, err := c.connection.PrepareBatch(batchCtx, "INSERT INTO http_log (timestamp, resource_id, bytes_sent, request_time_milli, response_status, cache_status, method, remote_addr, url, kafka_topic, kafka_partition, kafka_offset)")
	if err != nil {
		return err
	}

	for _, l := range logs {
		err := batch.Append(l.Timestamp, l.ResourceId, l.BytesSent, l.RequestTimeMilli, l.ResponseStatus, l.CacheStatus, l.Method, l.RemoteAddr, l.URL, l.KafkaTopic, l.KafkaPartition, l.KafkaOffset)

		if err != nil {
			batch.Abort()
			return err
		}
	}
	err = batch.Send()

	if err != nil {
		return err
	}

	return nil
}

func (c *Client) Close() error {
	return c.connection.Close()
}
