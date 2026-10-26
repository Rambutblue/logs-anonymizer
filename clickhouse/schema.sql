CREATE TABLE IF NOT EXISTS http_log (
    timestamp DateTime64(3),
    resource_id UInt64,
    bytes_sent UInt64,
    request_time_milli UInt64,
    response_status UInt16,
    cache_status LowCardinality(String),
    method LowCardinality(String),
    remote_addr String,
    url String,
    kafka_topic LowCardinality(String),
    kafka_partition UInt16,
    kafka_offset UInt64
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, resource_id, response_status, cache_status, remote_addr, kafka_topic, kafka_partition, kafka_offset)
PRIMARY KEY (timestamp, resource_id)
SETTINGS non_replicated_deduplication_window = 1000;

CREATE TABLE IF NOT EXISTS http_log_totals (
    minute DateTime,
    resource_id UInt64,
    response_status UInt16,
    cache_status LowCardinality(String),
    remote_addr String,
    bytes_sent UInt64,
    requests UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (resource_id, minute, response_status, cache_status, remote_addr)
SETTINGS non_replicated_deduplication_window = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS http_log_totals_mv TO http_log_totals AS
SELECT toStartOfMinute(timestamp) AS minute, resource_id, response_status,
       cache_status, remote_addr,
       sum(bytes_sent) AS bytes_sent, count() AS requests
FROM http_log
GROUP BY minute, resource_id, response_status, cache_status, remote_addr;
