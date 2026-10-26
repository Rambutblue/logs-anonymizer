package anonymize

import (
	"anonymizer/internal/model"

	"errors"
	"net/netip"
	"strings"
)

const maskedOctet = "X"

var ErrInvalidRemoteAddr = errors.New("remote address is not an IPv4 address")

func Anonymize(log model.Log) (model.Log, error) {
	ip, err := maskLastOctet(log.RemoteAddr)
	if err != nil {
		return model.Log{}, err
	}

	log.RemoteAddr = ip
	return log, nil
}

func maskLastOctet(addr string) (string, error) {
	lastDot := strings.LastIndexByte(addr, '.')
	if lastDot == -1 {
		return "", ErrInvalidRemoteAddr
	}

	prefix := addr[:lastDot+1]
	toValidate := addr
	if addr[lastDot+1:] == maskedOctet {
		toValidate = prefix + "0"
	}

	if parsed, err := netip.ParseAddr(toValidate); err != nil || !parsed.Is4() {
		return "", ErrInvalidRemoteAddr
	}

	return prefix + maskedOctet, nil
}
