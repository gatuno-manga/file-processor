package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

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

func TestKafkaAdapter_EmitSanitizedEvent(t *testing.T) {
	mw := &mockKafkaWriter{
		writeFunc: func(ctx context.Context, msgs ...kafka.Message) error {
			if len(msgs) != 1 {
				return errors.New("expected 1 message")
			}
			var event FileSanitizedEvent
			if err := json.Unmarshal(msgs[0].Value, &event); err != nil {
				return err
			}
			if event.Bucket != "test-bucket" || event.Key != "test-key" {
				return errors.New("unexpected event data")
			}
			return nil
		},
	}

	adapter := &KafkaAdapter{writer: mw}
	err := adapter.EmitSanitizedEvent(context.Background(), "test-key", "test-bucket")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestKafkaAdapter_Consume(t *testing.T) {
	event := ImageDownloadedEvent{Bucket: "test-bucket", Key: "test-key"}
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

	adapter := &KafkaAdapter{reader: mr, writer: mw}

	ctx, cancel := context.WithCancel(context.Background())
	var handled bool
	handler := func(ctx context.Context, key, bucket string) error {
		if key == "test-key" && bucket == "test-bucket" {
			handled = true
		}
		cancel() // Stop the loop
		return nil
	}

	adapter.Consume(ctx, handler)

	if !handled {
		t.Error("expected handler to be called")
	}
}
