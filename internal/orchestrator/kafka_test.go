package orchestrator

import (
	"context"
	"errors"
	"testing"

	"github.com/luis/file-processor/internal/pool"
	"github.com/luis/file-processor/internal/processor"
)

type mockStorage struct {
	downloadFunc func(ctx context.Context, bucket, key string) ([]byte, error)
	uploadFunc   func(ctx context.Context, bucket, key string, data []byte) error
	deleteFunc   func(ctx context.Context, bucket, key string) error
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

func (m *mockStorage) Delete(ctx context.Context, bucket, key string) error {
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, bucket, key)
	}
	return nil
}

func (m *mockStorage) Release(data []byte) {}

type mockProducer struct {
	emitFunc func(ctx context.Context, rawPath, targetBucket, targetPath string, metadata *processor.Metadata) error
}

func (m *mockProducer) EmitProcessingCompletedEvent(ctx context.Context, rawPath, targetBucket, targetPath string, metadata *processor.Metadata) error {
	if m.emitFunc != nil {
		return m.emitFunc(ctx, rawPath, targetBucket, targetPath, metadata)
	}
	return nil
}

func TestKafkaOrchestrator_Handle(t *testing.T) {
	pool.InitPool(1)
	pool.SetProcessFunc(func(data []byte, quality int, isBackfill bool) ([]byte, *processor.Metadata, error) {
		return []byte("sanitized"), &processor.Metadata{}, nil
	})

	ms := &mockStorage{
		downloadFunc: func(ctx context.Context, bucket, key string) ([]byte, error) {
			if bucket != "processing" || key != "ab/test.jpg" {
				return nil, errors.New("unexpected download arguments")
			}
			return []byte("original"), nil
		},
		uploadFunc: func(ctx context.Context, bucket, key string, data []byte) error {
			if bucket != "books" || key != "ab/test.webp" {
				return errors.New("unexpected upload arguments")
			}
			if string(data) != "sanitized" {
				return errors.New("unexpected data uploaded")
			}
			return nil
		},
		deleteFunc: func(ctx context.Context, bucket, key string) error {
			if bucket != "processing" || key != "ab/test.jpg" {
				return errors.New("unexpected delete arguments")
			}
			return nil
		},
	}
	mp := &mockProducer{
		emitFunc: func(ctx context.Context, rawPath, targetBucket, targetPath string, metadata *processor.Metadata) error {
			if rawPath != "processing/ab/test.jpg" || targetBucket != "books" || targetPath != "ab/test.webp" {
				return errors.New("unexpected event emitted")
			}
			return nil
		},
	}

	o := NewKafkaOrchestrator(ms, mp)
	err := o.Handle(context.Background(), "processing/ab/test.jpg", "books", "ab/test.webp", false)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}
