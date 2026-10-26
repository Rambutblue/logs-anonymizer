package clickhouse

import (
	"anonymizer/internal/httplog"

	"context"
	"errors"
)

var errNotImplemented = errors.New("not implemented")

type Config struct {
	Addr     string
	Database string
	Username string
	Password string
}

type Client struct {
	cfg Config
}

func New(ctx context.Context, cfg Config) (*Client, error) {
	return nil, errNotImplemented
}

func (c *Client) InsertBatch(ctx context.Context, logs []httplog.Log) error {
	return errNotImplemented
}

func (c *Client) Close() error {
	return errNotImplemented
}
