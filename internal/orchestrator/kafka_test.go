package orchestrator

import (
	"context"
	"errors"
	"testing"

	"github.com/luis/file-processor/internal/pool"
)

type mockStorage struct {
	downloadFunc func(ctx context.Context, bucket, key string) ([]byte, error)
	uploadFunc   func(ctx context.Context, bucket, key string, data []byte) error
}

func (m *mockStorage) Download(ctx context.Context, bucket, key string) ([]byte, error) {
	if m.downloadFunc != nil {
		return m.downloadFunc(ctx, bucket, key)
	}
	return nil, nil
}

func (m *mockStorage) Upload(ctx context.Context, bucket, key string, data []byte) error {
	if m.uploadFunc != nil {
		return m.uploadFunc(ctx, bucket, key, data)
	}
	return nil
}

type mockProducer struct {
	emitFunc func(ctx context.Context, key, bucket string) error
}

func (m *mockProducer) EmitSanitizedEvent(ctx context.Context, key, bucket string) error {
	if m.emitFunc != nil {
		return m.emitFunc(ctx, key, bucket)
	}
	return nil
}

func TestKafkaOrchestrator_Handle(t *testing.T) {
	// Initialize pool for test
	pool.InitPool(1)
	pool.SetProcessFunc(func(data []byte) ([]byte, error) {
		return []byte("sanitized"), nil
	})

	ms := &mockStorage{
		downloadFunc: func(ctx context.Context, bucket, key string) ([]byte, error) {
			return []byte("original"), nil
		},
		uploadFunc: func(ctx context.Context, bucket, key string, data []byte) error {
			if string(data) != "sanitized" {
				return errors.New("unexpected data uploaded")
			}
			return nil
		},
	}
	mp := &mockProducer{
		emitFunc: func(ctx context.Context, key, bucket string) error {
			if key != "test-key.sanitized.webp" {
				return errors.New("unexpected key emitted")
			}
			return nil
		},
	}

	o := NewKafkaOrchestrator(ms, mp)
	err := o.Handle(context.Background(), "test-key", "test-bucket")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}
