package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/luis/file-processor/internal/port"
	"github.com/luis/file-processor/internal/processor"
	"github.com/segmentio/kafka-go"
)

type mockKafkaWriter struct {
	writeFunc func(ctx context.Context, msgs ...kafka.Message) error
	closeFunc func() error
}

func (m *mockKafkaWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	if m.writeFunc != nil {
		return m.writeFunc(ctx, msgs...)
	}
	return nil
}

func (m *mockKafkaWriter) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

func (m *mockKafkaWriter) Stats() kafka.WriterStats { return kafka.WriterStats{} }

type mockKafkaReader struct {
	fetchFunc  func(ctx context.Context) (kafka.Message, error)
	commitFunc func(ctx context.Context, msgs ...kafka.Message) error
	closeFunc  func() error
}

func (m *mockKafkaReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if m.fetchFunc != nil {
		return m.fetchFunc(ctx)
	}
	return kafka.Message{}, nil
}

func (m *mockKafkaReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	if m.commitFunc != nil {
		return m.commitFunc(ctx, msgs...)
	}
	return nil
}

func (m *mockKafkaReader) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

func (m *mockKafkaReader) Stats() kafka.ReaderStats { return kafka.ReaderStats{} }

func TestKafkaAdapter_EmitProcessingCompletedEvent(t *testing.T) {
	mw := &mockKafkaWriter{
		writeFunc: func(ctx context.Context, msgs ...kafka.Message) error {
			if len(msgs) != 1 {
				return errors.New("expected 1 message")
			}
			var event ImageProcessingCompletedEvent
			if err := json.Unmarshal(msgs[0].Value, &event); err != nil {
				return err
			}
			if event.RawPath != "processing/test.jpg" || event.OriginalUrl != "https://example.com/test.jpg" || event.TargetBucket != "books" || len(event.Results) != 1 {
				return errors.New("unexpected event data")
			}
			if event.Results[0].TargetPath != "test.webp" || event.Results[0].Metadata == nil || event.Results[0].Metadata.MimeType != "image/webp" {
				return errors.New("unexpected metadata or target path")
			}
			return nil
		},
	}

	adapter := &KafkaAdapter{writer: mw}
	metadata := &processor.Metadata{MimeType: "image/webp"}
	results := []port.ProcessingResult{
		{
			TargetPath: "test.webp",
			Metadata:   metadata,
		},
	}
	err := adapter.EmitProcessingCompletedEvent(context.Background(), "processing/test.jpg", "https://example.com/test.jpg", "books", results)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestKafkaAdapter_Ping(t *testing.T) {
	// Note: Testing actual dial is hard without real server,
	// but we can at least check it doesn't panic and handles empty brokers.
	adapter := &KafkaAdapter{brokers: []string{}}
	if err := adapter.Ping(context.Background()); err != nil {
		t.Errorf("expected no error for empty brokers, got %v", err)
	}
}

func TestKafkaAdapter_Consume(t *testing.T) {
	event := ImageProcessingRequestedEvent{
		RawBucket:    "processing",
		RawPath:      "test.jpg",
		OriginalUrl:  "https://example.com/test.jpg",
		TargetBucket: "books",
		TargetPath:   "test.webp",
		IsBackfill:   true,
	}
	payload, _ := json.Marshal(event)

	mr := &mockKafkaReader{
		fetchFunc: func(ctx context.Context) (kafka.Message, error) {
			select {
			case <-ctx.Done():
				return kafka.Message{}, ctx.Err()
			default:
				return kafka.Message{Value: payload}, nil
			}
		},
		commitFunc: func(ctx context.Context, msgs ...kafka.Message) error {
			return nil
		},
		closeFunc: func() error {
			return nil
		},
	}
	mw := &mockKafkaWriter{
		closeFunc: func() error {
			return nil
		},
	}

	adapter := &KafkaAdapter{
		reader:            mr,
		writer:            mw,
		dlqWriter:         mw,
		imageSemaphore:    make(chan struct{}, 1),
		documentSemaphore: make(chan struct{}, 1),
		maxTries:          1,
		retryBackoff:      time.Millisecond,
		processTimeout:    time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())
	var handled bool
	handler := func(ctx context.Context, rawBucket, rawPath, originalUrl, targetBucket, targetPath string, isBackfill bool) error {
		if rawBucket == "processing" && rawPath == "test.jpg" && originalUrl == "https://example.com/test.jpg" && targetBucket == "books" && targetPath == "test.webp" && isBackfill {
			handled = true
		}
		cancel()
		return nil
	}

	adapter.Consume(ctx, handler)

	if !handled {
		t.Error("expected handler to be called")
	}
}

// headerValue returns the value of the named header, or "" if absent.
func headerValue(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func TestEmitProcessingCompletedEvent_NilMetadata(t *testing.T) {
	mw := &mockKafkaWriter{
		writeFunc: func(ctx context.Context, msgs ...kafka.Message) error {
			var event ImageProcessingCompletedEvent
			if err := json.Unmarshal(msgs[0].Value, &event); err != nil {
				return err
			}
			if event.Results[0].Metadata == nil {
				return errors.New("expected a non-nil placeholder Metadata in the emitted event")
			}
			return nil
		},
	}

	adapter := &KafkaAdapter{writer: mw}
	results := []port.ProcessingResult{
		{TargetPath: "test.webp", Metadata: nil},
	}

	// Must not panic: this is exactly the scenario from C1 (metadata extraction
	// failure upstream leaves ProcessingResult.Metadata nil).
	err := adapter.EmitProcessingCompletedEvent(context.Background(), "processing/test.jpg", "https://example.com/test.jpg", "books", results)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestResolveStartOffset(t *testing.T) {
	if got := resolveStartOffset(false); got != kafka.LastOffset {
		t.Errorf("resolveStartOffset(false) = %v, want kafka.LastOffset", got)
	}
	if got := resolveStartOffset(true); got != kafka.FirstOffset {
		t.Errorf("resolveStartOffset(true) = %v, want kafka.FirstOffset", got)
	}
}

func TestHandleWithRetry_ExhaustsRetriesToDLQ(t *testing.T) {
	var dlqMsgs []kafka.Message
	var commitCount int
	var callCount int

	mw := &mockKafkaWriter{
		writeFunc: func(ctx context.Context, msgs ...kafka.Message) error {
			dlqMsgs = append(dlqMsgs, msgs...)
			return nil
		},
	}
	mr := &mockKafkaReader{
		commitFunc: func(ctx context.Context, msgs ...kafka.Message) error {
			commitCount++
			return nil
		},
	}

	adapter := &KafkaAdapter{maxTries: 3, retryBackoff: time.Millisecond, processTimeout: time.Second}
	handlerErr := errors.New("boom")
	msg := kafka.Message{Key: []byte("k"), Value: []byte("v"), Topic: "input", Partition: 2, Offset: 42}

	adapter.handleWithRetry(context.Background(), mr, mw, msg, "handler", func(ctx context.Context) error {
		callCount++
		return handlerErr
	})

	if callCount != 3 {
		t.Errorf("expected 3 attempts (maxTries), got %d", callCount)
	}
	if len(dlqMsgs) != 1 {
		t.Fatalf("expected exactly 1 DLQ message, got %d", len(dlqMsgs))
	}
	if commitCount != 1 {
		t.Errorf("expected exactly 1 commit, got %d", commitCount)
	}

	dlq := dlqMsgs[0]
	if string(dlq.Key) != "k" || string(dlq.Value) != "v" {
		t.Errorf("DLQ message must preserve key/value verbatim: key=%q value=%q", dlq.Key, dlq.Value)
	}
	if headerValue(dlq.Headers, "x-error") != handlerErr.Error() {
		t.Errorf("unexpected x-error header: %q", headerValue(dlq.Headers, "x-error"))
	}
	if headerValue(dlq.Headers, "x-error-stage") != "handler" {
		t.Errorf("unexpected x-error-stage header: %q", headerValue(dlq.Headers, "x-error-stage"))
	}
	if headerValue(dlq.Headers, "x-original-topic") != "input" {
		t.Errorf("unexpected x-original-topic header: %q", headerValue(dlq.Headers, "x-original-topic"))
	}
	if headerValue(dlq.Headers, "x-original-partition") != strconv.Itoa(2) {
		t.Errorf("unexpected x-original-partition header: %q", headerValue(dlq.Headers, "x-original-partition"))
	}
	if headerValue(dlq.Headers, "x-original-offset") != strconv.FormatInt(42, 10) {
		t.Errorf("unexpected x-original-offset header: %q", headerValue(dlq.Headers, "x-original-offset"))
	}
	if headerValue(dlq.Headers, "x-failed-at") == "" {
		t.Errorf("expected a non-empty x-failed-at header")
	}
}

func TestHandleWithRetry_SucceedsAfterOneRetry(t *testing.T) {
	var dlqCount int
	var commitCount int
	var callCount int

	mw := &mockKafkaWriter{
		writeFunc: func(ctx context.Context, msgs ...kafka.Message) error {
			dlqCount++
			return nil
		},
	}
	mr := &mockKafkaReader{
		commitFunc: func(ctx context.Context, msgs ...kafka.Message) error {
			commitCount++
			return nil
		},
	}

	adapter := &KafkaAdapter{maxTries: 3, retryBackoff: time.Millisecond, processTimeout: time.Second}
	msg := kafka.Message{Key: []byte("k"), Value: []byte("v")}

	adapter.handleWithRetry(context.Background(), mr, mw, msg, "handler", func(ctx context.Context) error {
		callCount++
		if callCount == 1 {
			return errors.New("transient")
		}
		return nil
	})

	if callCount != 2 {
		t.Errorf("expected 2 attempts, got %d", callCount)
	}
	if dlqCount != 0 {
		t.Errorf("expected 0 DLQ messages, got %d", dlqCount)
	}
	if commitCount != 1 {
		t.Errorf("expected 1 commit, got %d", commitCount)
	}
}

func TestHandleWithRetry_ContextCancelledDuringRetryLeavesUncommitted(t *testing.T) {
	var dlqCount int
	var commitCount int

	mw := &mockKafkaWriter{
		writeFunc: func(ctx context.Context, msgs ...kafka.Message) error {
			dlqCount++
			return nil
		},
	}
	mr := &mockKafkaReader{
		commitFunc: func(ctx context.Context, msgs ...kafka.Message) error {
			commitCount++
			return nil
		},
	}

	adapter := &KafkaAdapter{maxTries: 5, retryBackoff: 50 * time.Millisecond, processTimeout: time.Second}
	msg := kafka.Message{Key: []byte("k"), Value: []byte("v")}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	adapter.handleWithRetry(ctx, mr, mw, msg, "handler", func(ctx context.Context) error {
		return errors.New("always fails")
	})

	if commitCount != 0 {
		t.Errorf("expected 0 commits after cancellation mid-retry, got %d", commitCount)
	}
	if dlqCount != 0 {
		t.Errorf("expected 0 DLQ messages after cancellation, got %d", dlqCount)
	}
}

func TestHandleWithRetry_TimeoutRoutesToDLQWithTimeoutStage(t *testing.T) {
	var dlqMsgs []kafka.Message
	var commitCount int

	mw := &mockKafkaWriter{
		writeFunc: func(ctx context.Context, msgs ...kafka.Message) error {
			dlqMsgs = append(dlqMsgs, msgs...)
			return nil
		},
	}
	mr := &mockKafkaReader{
		commitFunc: func(ctx context.Context, msgs ...kafka.Message) error {
			commitCount++
			return nil
		},
	}

	adapter := &KafkaAdapter{maxTries: 1, retryBackoff: time.Millisecond, processTimeout: 10 * time.Millisecond}
	msg := kafka.Message{Key: []byte("k"), Value: []byte("v")}

	var gotErr error
	adapter.handleWithRetry(context.Background(), mr, mw, msg, "handler", func(ctx context.Context) error {
		<-ctx.Done() // block past the per-attempt timeout
		gotErr = ctx.Err()
		return ctx.Err()
	})

	if !errors.Is(gotErr, context.DeadlineExceeded) {
		t.Errorf("expected handler to observe context.DeadlineExceeded, got %v", gotErr)
	}
	if len(dlqMsgs) != 1 {
		t.Fatalf("expected exactly 1 DLQ message, got %d", len(dlqMsgs))
	}
	if headerValue(dlqMsgs[0].Headers, "x-error-stage") != "timeout" {
		t.Errorf("expected x-error-stage=timeout, got %q", headerValue(dlqMsgs[0].Headers, "x-error-stage"))
	}
	if commitCount != 1 {
		t.Errorf("expected 1 commit, got %d", commitCount)
	}
}

func TestHandleWithRetry_PanicIsRecoveredAndDLQd(t *testing.T) {
	var dlqMsgs []kafka.Message
	var commitCount int

	mw := &mockKafkaWriter{
		writeFunc: func(ctx context.Context, msgs ...kafka.Message) error {
			dlqMsgs = append(dlqMsgs, msgs...)
			return nil
		},
	}
	mr := &mockKafkaReader{
		commitFunc: func(ctx context.Context, msgs ...kafka.Message) error {
			commitCount++
			return nil
		},
	}

	adapter := &KafkaAdapter{maxTries: 3, retryBackoff: time.Millisecond, processTimeout: time.Second}
	msg := kafka.Message{Key: []byte("k"), Value: []byte("v"), Topic: "input", Partition: 1, Offset: 9}

	didPanicEscape := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				didPanicEscape = true
			}
		}()
		adapter.handleWithRetry(context.Background(), mr, mw, msg, "handler", func(ctx context.Context) error {
			panic("boom")
		})
	}()

	if didPanicEscape {
		t.Fatal("panic escaped handleWithRetry — recover() did not catch it")
	}
	if len(dlqMsgs) != 1 {
		t.Fatalf("expected exactly 1 DLQ message after panic, got %d", len(dlqMsgs))
	}
	if headerValue(dlqMsgs[0].Headers, "x-error-stage") != "panic" {
		t.Errorf("expected x-error-stage=panic, got %q", headerValue(dlqMsgs[0].Headers, "x-error-stage"))
	}
	if commitCount != 1 {
		t.Errorf("expected 1 commit after panic-DLQ, got %d", commitCount)
	}
}

func TestConsume_MalformedMessageGoesToDLQ(t *testing.T) {
	malformed := kafka.Message{Value: []byte("not json"), Key: []byte("key1"), Topic: "input", Partition: 0, Offset: 7}

	ctx, cancel := context.WithCancel(context.Background())
	fetchCount := 0
	var dlqMsgs []kafka.Message
	var commitCount int

	mr := &mockKafkaReader{
		fetchFunc: func(ctx context.Context) (kafka.Message, error) {
			fetchCount++
			if fetchCount == 1 {
				return malformed, nil
			}
			cancel()
			return kafka.Message{}, ctx.Err()
		},
		commitFunc: func(ctx context.Context, msgs ...kafka.Message) error {
			commitCount++
			return nil
		},
	}
	mdlq := &mockKafkaWriter{
		writeFunc: func(ctx context.Context, msgs ...kafka.Message) error {
			dlqMsgs = append(dlqMsgs, msgs...)
			return nil
		},
	}

	adapter := &KafkaAdapter{
		reader:            mr,
		dlqWriter:         mdlq,
		imageSemaphore:    make(chan struct{}, 1),
		documentSemaphore: make(chan struct{}, 1),
		maxTries:          1,
		retryBackoff:      time.Millisecond,
		processTimeout:    time.Second,
	}

	handler := func(ctx context.Context, rawBucket, rawPath, originalUrl, targetBucket, targetPath string, isBackfill bool) error {
		t.Fatal("handler should never be invoked for a malformed message")
		return nil
	}

	adapter.Consume(ctx, handler)

	if len(dlqMsgs) != 1 {
		t.Fatalf("expected malformed message to be routed to the DLQ exactly once, got %d", len(dlqMsgs))
	}
	if string(dlqMsgs[0].Value) != "not json" || string(dlqMsgs[0].Key) != "key1" {
		t.Errorf("DLQ message must preserve the malformed payload/key verbatim")
	}
	if headerValue(dlqMsgs[0].Headers, "x-error-stage") != "unmarshal" {
		t.Errorf("expected x-error-stage=unmarshal, got %q", headerValue(dlqMsgs[0].Headers, "x-error-stage"))
	}
	if commitCount != 1 {
		t.Errorf("expected the malformed message to be committed so the partition doesn't stall, got %d commits", commitCount)
	}
}

func TestConsume_PanicRecoversAndProcessesNextMessage(t *testing.T) {
	p1, _ := json.Marshal(ImageProcessingRequestedEvent{RawBucket: "b", RawPath: "1.jpg"})
	p2, _ := json.Marshal(ImageProcessingRequestedEvent{RawBucket: "b", RawPath: "2.jpg"})
	msgs := []kafka.Message{
		{Value: p1, Offset: 1},
		{Value: p2, Offset: 2},
	}

	ctx, cancel := context.WithCancel(context.Background())
	fetchCount := 0
	var commitCount int
	var dlqCount int

	mr := &mockKafkaReader{
		fetchFunc: func(ctx context.Context) (kafka.Message, error) {
			if fetchCount < len(msgs) {
				m := msgs[fetchCount]
				fetchCount++
				return m, nil
			}
			cancel()
			return kafka.Message{}, ctx.Err()
		},
		commitFunc: func(ctx context.Context, msgs ...kafka.Message) error {
			commitCount++
			return nil
		},
	}
	mdlq := &mockKafkaWriter{
		writeFunc: func(ctx context.Context, msgs ...kafka.Message) error {
			dlqCount++
			return nil
		},
	}

	adapter := &KafkaAdapter{
		reader:            mr,
		dlqWriter:         mdlq,
		imageSemaphore:    make(chan struct{}, 1),
		documentSemaphore: make(chan struct{}, 1),
		maxTries:          1,
		retryBackoff:      time.Millisecond,
		processTimeout:    time.Second,
	}

	var secondHandled bool
	handler := func(ctx context.Context, rawBucket, rawPath, originalUrl, targetBucket, targetPath string, isBackfill bool) error {
		if rawPath == "1.jpg" {
			panic("boom")
		}
		if rawPath == "2.jpg" {
			secondHandled = true
		}
		return nil
	}

	// Must not crash the test process: handleWithRetry recovers the panic.
	adapter.Consume(ctx, handler)

	if !secondHandled {
		t.Error("expected the second message to be handled after the first message's handler panicked")
	}
	if dlqCount != 1 {
		t.Errorf("expected exactly 1 DLQ message (from the panic), got %d", dlqCount)
	}
	if commitCount != 2 {
		t.Errorf("expected 2 commits (panic-DLQ for msg 1, success for msg 2), got %d", commitCount)
	}
}
