package logic

import (
	"anonymizer/internal/httplog"
	"strings"
)

func Anonymize(log httplog.Log) httplog.Log {
	ip := log.RemoteAddr
	// Assuming the address is IPv4 and is x.x.x.x 
	if lastDot := strings.LastIndexByte(ip, '.'); lastDot != -1 {
    	ip = ip[:lastDot+1] + "x"
	}
	log.RemoteAddr = ip
	return log
	
}