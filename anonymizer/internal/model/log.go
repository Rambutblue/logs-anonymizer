package model

import "time"

type Log struct {
	Timestamp           time.Time
	TimestampEpochMilli uint64
	ResourceId          uint64
	BytesSent           uint64
	RequestTimeMilli    uint64
	ResponseStatus      uint16
	CacheStatus         string
	Method              string
	RemoteAddr          string
	URL                 string
	KafkaTopic          string
	KafkaPartition      uint16
	KafkaOffset         uint64
}
