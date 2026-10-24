package httplog

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
}