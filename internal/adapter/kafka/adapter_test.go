package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

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

	adapter := &KafkaAdapter{reader: mr, writer: mw, imageSemaphore: make(chan struct{}, 1), documentSemaphore: make(chan struct{}, 1)}

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
