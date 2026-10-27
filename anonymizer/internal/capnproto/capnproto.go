package capnproto

import (
	"anonymizer/internal/httplog"
	"anonymizer/internal/model"
	"capnproto.org/go/capnp/v3"
	"errors"
	"math"
	"time"
)

func GetLog(msg *[]byte) (model.Log, error) {
	cm, err := capnp.Unmarshal(*msg)
	if err != nil {
		return model.Log{}, err
	}
	log, err := httplog.ReadRootHttpLogRecord(cm)
	if err != nil {
		return model.Log{}, err
	}

	cacheStatus, err := log.CacheStatus()
	if err != nil {
		return model.Log{}, err
	}

	method, err := log.Method()
	if err != nil {
		return model.Log{}, err
	}

	remoteAddr, err := log.RemoteAddr()
	if err != nil {
		return model.Log{}, err
	}

	url, err := log.Url()
	if err != nil {
		return model.Log{}, err
	}

	var timestamp time.Time
	if log.TimestampEpochMilli() <= uint64(math.MaxInt64) {
		timestamp = time.UnixMilli(int64(log.TimestampEpochMilli())).UTC()
	} else {
		return model.Log{}, errors.New("timestamp exceeds maximum int64 value")
	}

	return model.Log{
		Timestamp:           timestamp,
		TimestampEpochMilli: log.TimestampEpochMilli(),
		ResourceId:          log.ResourceId(),
		BytesSent:           log.BytesSent(),
		RequestTimeMilli:    log.RequestTimeMilli(),
		ResponseStatus:      log.ResponseStatus(),
		CacheStatus:         cacheStatus,
		Method:              method,
		RemoteAddr:          remoteAddr,
		URL:                 url,
	}, nil
}
