package model

type Log struct {
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
