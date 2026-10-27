package anonymize

import (
	"anonymizer/internal/model"

	"errors"
	"testing"
	"time"
)

func TestAnonymizeMasksLastOctet(t *testing.T) {
	tests := []struct {
		name string
		addr string
		want string
	}{
		{"single digit octet", "1.2.3.4", "1.2.3.X"},
		{"three digit octet", "177.33.54.104", "177.33.54.X"},
		{"zero octet", "10.0.0.0", "10.0.0.X"},
		{"already masked", "1.2.3.X", "1.2.3.X"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Anonymize(model.Log{RemoteAddr: tt.addr})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.RemoteAddr != tt.want {
				t.Errorf("RemoteAddr = %q, want %q", got.RemoteAddr, tt.want)
			}
		})
	}
}

func TestAnonymizeKeepsOtherFields(t *testing.T) {
	in := model.Log{
		Timestamp:        time.UnixMilli(1792841824754).UTC(),
		ResourceId:       420575753,
		BytesSent:        883321,
		RequestTimeMilli: 3313,
		ResponseStatus:   403,
		CacheStatus:      "STALE",
		Method:           "HEAD",
		RemoteAddr:       "177.33.54.104",
		URL:              "/images/preview/media/fetch/b12ia4grz7lgrjq9ul45w.json",
		KafkaTopic:       "http_log",
		KafkaPartition:   2,
		KafkaOffset:      42,
	}

	want := in
	want.RemoteAddr = "177.33.54.X"

	got, err := Anonymize(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestAnonymizeRejectsInvalidAddr(t *testing.T) {
	tests := []struct {
		name string
		addr string
	}{
		{"empty", ""},
		{"not an address", "localhost"},
		{"too few octets", "1.2.3"},
		{"too many octets", "1.2.3.4.5"},
		{"octet out of range", "1.2.3.256"},
		{"missing last octet", "1.2.3."},
		{"masked octet in the middle", "1.X.3.4"},
		{"ipv6", "2001:db8::1"},
		{"ipv6 loopback", "::1"},
		{"ipv4 mapped ipv6", "::ffff:1.2.3.4"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Anonymize(model.Log{RemoteAddr: tt.addr, URL: "/keep"})
			if !errors.Is(err, ErrInvalidRemoteAddr) {
				t.Fatalf("error = %v, want %v", err, ErrInvalidRemoteAddr)
			}
			if got != (model.Log{}) {
				t.Errorf("got %+v, want zero Log on error", got)
			}
		})
	}
}
