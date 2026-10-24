package capnproto

import (
	"anonymizer/internal/httplog"
	"capnproto.org/go/capnp/v3"
)

func GetLog(msg *[]byte) (httplog.Log, error) {
	cm, err := capnp.Unmarshal(*msg)
	if err != nil {
		return httplog.Log{}, err
	}
	log, err := httplog.ReadRootHttpLogRecord(cm)
	if err != nil {
		return httplog.Log{}, err
	}

	cacheStatus, err := log.CacheStatus()
	if err != nil {
		return httplog.Log{}, err
	}
	
    method, err := log.Method()
	if err != nil {
		return httplog.Log{}, err
	}
	
	remoteAddr, err := log.RemoteAddr()
	if err != nil {
		return httplog.Log{}, err
	}
	
	url, err := log.Url()
	if err != nil {
		return httplog.Log{}, err
	}

	return httplog.Log{
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