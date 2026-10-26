package pipeline

import (
	"anonymizer/internal/consumer"
	"anonymizer/internal/httplog"
	"anonymizer/internal/model"

	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"capnproto.org/go/capnp/v3"
)

const testTopic = "http_log"

type fakeBroker struct {
	mu      sync.Mutex
	events  []string
	inserts [][]model.Log
	commits [][]consumer.Message

	insertErr error
	inserted  chan struct{}
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{inserted: make(chan struct{}, 16)}
}

func (f *fakeBroker) Fetch(ctx context.Context) (consumer.Message, error) {
	<-ctx.Done()
	return consumer.Message{}, ctx.Err()
}

func (f *fakeBroker) Commit(_ context.Context, msgs []consumer.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "commit")
	f.commits = append(f.commits, append([]consumer.Message(nil), msgs...))
	return nil
}

func (f *fakeBroker) InsertBatch(_ context.Context, logs []model.Log) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.insertErr != nil {
		return f.insertErr
	}
	f.events = append(f.events, "insert")
	f.inserts = append(f.inserts, append([]model.Log(nil), logs...))
	f.inserted <- struct{}{}
	return nil
}

func (f *fakeBroker) snapshot() (events []string, inserts [][]model.Log, commits [][]consumer.Message) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.events...), f.inserts, f.commits
}

func newTestProcessor(broker *fakeBroker, sizeLimit int, timeLimit time.Duration) *Processor {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewProcessor(logger, broker, broker, sizeLimit, timeLimit)
}

func encodeLog(t *testing.T, remoteAddr string) []byte {
	t.Helper()

	cm, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		t.Fatal(err)
	}
	record, err := httplog.NewRootHttpLogRecord(seg)
	if err != nil {
		t.Fatal(err)
	}
	record.SetTimestampEpochMilli(1792841824754)
	record.SetResourceId(420575753)
	record.SetBytesSent(883321)
	record.SetRequestTimeMilli(3313)
	record.SetResponseStatus(403)
	for _, set := range []error{
		record.SetCacheStatus("STALE"),
		record.SetMethod("HEAD"),
		record.SetRemoteAddr(remoteAddr),
		record.SetUrl("/index.json"),
	} {
		if set != nil {
			t.Fatal(set)
		}
	}

	data, err := cm.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func message(offset int64, value []byte) consumer.Message {
	return consumer.Message{Topic: testTopic, Partition: 1, Offset: offset, Value: value}
}

func filledChannel(msgs ...consumer.Message) chan consumer.Message {
	channel := make(chan consumer.Message, len(msgs))
	for _, msg := range msgs {
		channel <- msg
	}
	close(channel)
	return channel
}

func TestRunFlushesWhenSizeLimitReached(t *testing.T) {
	broker := newFakeBroker()
	channel := filledChannel(
		message(0, encodeLog(t, "1.2.3.4")),
		message(1, encodeLog(t, "1.2.3.5")),
		message(2, encodeLog(t, "1.2.3.6")),
	)

	if err := newTestProcessor(broker, 2, time.Hour).Run(context.Background(), channel); err != nil {
		t.Fatalf("Run returned %v", err)
	}

	events, inserts, commits := broker.snapshot()
	if len(inserts) != 2 || len(inserts[0]) != 2 || len(inserts[1]) != 1 {
		t.Fatalf("insert sizes = %v, want [2 1] (full batch, then remainder flushed on close)", inserts)
	}
	if len(commits) != 2 || len(commits[0]) != 2 || len(commits[1]) != 1 {
		t.Fatalf("commit sizes = %v, want [2 1]", commits)
	}
	if want := []string{"insert", "commit", "insert", "commit"}; !equal(events, want) {
		t.Errorf("events = %v, want %v (commit only after insert)", events, want)
	}
}

func TestRunAnonymizesAndAddsKafkaMetadata(t *testing.T) {
	broker := newFakeBroker()
	channel := filledChannel(message(7, encodeLog(t, "177.33.54.104")))

	if err := newTestProcessor(broker, 10, time.Hour).Run(context.Background(), channel); err != nil {
		t.Fatalf("Run returned %v", err)
	}

	_, inserts, _ := broker.snapshot()
	if len(inserts) != 1 || len(inserts[0]) != 1 {
		t.Fatalf("inserts = %v, want one batch with one log", inserts)
	}
	got := inserts[0][0]
	if got.RemoteAddr != "177.33.54.X" {
		t.Errorf("RemoteAddr = %q, want 177.33.54.X", got.RemoteAddr)
	}
	if got.KafkaTopic != testTopic || got.KafkaPartition != 1 || got.KafkaOffset != 7 {
		t.Errorf("kafka metadata = %s/%d/%d, want %s/1/7", got.KafkaTopic, got.KafkaPartition, got.KafkaOffset, testTopic)
	}
	if got.ResourceId != 420575753 || got.URL != "/index.json" {
		t.Errorf("payload fields were not decoded: %+v", got)
	}
}

func TestRunFlushesWhenTimeLimitReached(t *testing.T) {
	broker := newFakeBroker()
	channel := make(chan consumer.Message)
	done := make(chan error, 1)
	go func() {
		done <- newTestProcessor(broker, 100, 20*time.Millisecond).Run(context.Background(), channel)
	}()

	channel <- message(0, encodeLog(t, "1.2.3.4"))

	select {
	case <-broker.inserted:
	case <-time.After(2 * time.Second):
		t.Fatal("batch was not flushed after the time limit")
	}

	_, inserts, commits := broker.snapshot()
	if len(inserts) != 1 || len(inserts[0]) != 1 || len(commits) != 1 || len(commits[0]) != 1 {
		t.Errorf("inserts = %v, commits = %v, want one single-log batch committed", inserts, commits)
	}

	close(channel)
	if err := <-done; err != nil {
		t.Fatalf("Run returned %v", err)
	}
}

func TestRunCommitsDroppedMessagesWithoutInsertingThem(t *testing.T) {
	broker := newFakeBroker()
	channel := filledChannel(
		message(0, []byte("not a capnp message")),
		message(1, encodeLog(t, "not-an-ip")),
		message(2, encodeLog(t, "1.2.3.4")),
	)

	if err := newTestProcessor(broker, 10, time.Hour).Run(context.Background(), channel); err != nil {
		t.Fatalf("Run returned %v", err)
	}

	_, inserts, commits := broker.snapshot()
	if len(inserts) != 1 || len(inserts[0]) != 1 || inserts[0][0].KafkaOffset != 2 {
		t.Fatalf("inserts = %v, want only the valid log (offset 2)", inserts)
	}
	if len(commits) != 1 || len(commits[0]) != 3 {
		t.Errorf("commits = %v, want all 3 messages committed so dropped ones are not refetched", commits)
	}
}

func TestRunDoesNotCommitWhenInsertFails(t *testing.T) {
	broker := newFakeBroker()
	broker.insertErr = errors.New("clickhouse is down")
	channel := filledChannel(message(0, encodeLog(t, "1.2.3.4")))

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)

	err := newTestProcessor(broker, 1, time.Hour).Run(ctx, channel)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}

	_, _, commits := broker.snapshot()
	if len(commits) != 0 {
		t.Errorf("commits = %v, want none while the insert keeps failing", commits)
	}
}

func TestRunWithEmptyBatchOnCloseReturnsNil(t *testing.T) {
	broker := newFakeBroker()

	if err := newTestProcessor(broker, 10, time.Hour).Run(context.Background(), filledChannel()); err != nil {
		t.Fatalf("Run returned %v", err)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
