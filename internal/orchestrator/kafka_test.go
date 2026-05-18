package orchestrator

import (
	"context"
	"errors"
	"testing"

	"github.com/luis/file-processor/internal/pool"
	"github.com/luis/file-processor/internal/port"
	"github.com/luis/file-processor/internal/processor"
)

type mockStorage struct {
	downloadFunc func(ctx context.Context, bucket, key string) ([]byte, error)
	uploadFunc   func(ctx context.Context, bucket, key string, data []byte, contentType string) error
	deleteFunc   func(ctx context.Context, bucket, key string) error
}

func (m *mockStorage) Download(ctx context.Context, bucket, key string) ([]byte, error) {
	if m.downloadFunc != nil {
		return m.downloadFunc(ctx, bucket, key)
	}
	return nil, nil
}

func (m *mockStorage) Upload(ctx context.Context, bucket, key string, data []byte, contentType string) error {
	if m.uploadFunc != nil {
		return m.uploadFunc(ctx, bucket, key, data, contentType)
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
	emitFunc func(ctx context.Context, rawPath, originalUrl, targetBucket string, results []port.ProcessingResult) error
}

func (m *mockProducer) EmitProcessingCompletedEvent(ctx context.Context, rawPath, originalUrl, targetBucket string, results []port.ProcessingResult) error {
	if m.emitFunc != nil {
		return m.emitFunc(ctx, rawPath, originalUrl, targetBucket, results)
	}
	return nil
}

func (m *mockProducer) EmitDocumentProcessingCompletedEvent(ctx context.Context, rawPath, targetBucket, targetPath string, metadata *port.DocumentMetadata) error {
	return nil
}

func TestKafkaOrchestrator_Handle(t *testing.T) {
	testPool := pool.NewWorkerPool(1)
	testPool.SetProcessFunc(func(data []byte, quality int, isBackfill bool) ([]processor.ProcessedResult, error) {
		return []processor.ProcessedResult{{Data: []byte("sanitized"), Metadata: &processor.Metadata{}}}, nil
	})

	ms := &mockStorage{
		downloadFunc: func(ctx context.Context, bucket, key string) ([]byte, error) {
			if bucket != "processing" || key != "ab/test.jpg" {
				return nil, errors.New("unexpected download arguments")
			}
			return []byte("original"), nil
		},
		uploadFunc: func(ctx context.Context, bucket, key string, data []byte, contentType string) error {
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
		emitFunc: func(ctx context.Context, rawPath, originalUrl, targetBucket string, results []port.ProcessingResult) error {
			if rawPath != "ab/test.jpg" || originalUrl != "https://example.com/test.jpg" || targetBucket != "books" || len(results) != 1 || results[0].TargetPath != "ab/test.webp" {
				return errors.New("unexpected event emitted")
			}
			return nil
		},
	}

	o := NewKafkaOrchestrator(ms, mp, testPool)
	err := o.Handle(context.Background(), "processing", "ab/test.jpg", "https://example.com/test.jpg", "books", "ab/test.webp", false)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}
