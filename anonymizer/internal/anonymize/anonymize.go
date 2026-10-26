package anonymize

import (
	"anonymizer/internal/model"
	"strings"
)

func Anonymize(log model.Log) model.Log {
	ip := log.RemoteAddr
	// Assuming the address is IPv4 and is x.x.x.x
	if lastDot := strings.LastIndexByte(ip, '.'); lastDot != -1 {
		ip = ip[:lastDot+1] + "X"
	}
	log.RemoteAddr = ip
	return log

}
