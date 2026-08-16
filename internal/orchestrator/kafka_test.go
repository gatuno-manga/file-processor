package orchestrator

import (
	"context"
	"errors"
	"sync"
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
	testPool := pool.NewWorkerPool(1, processor.DefaultConfig)
	testPool.SetProcessFunc(func(data []byte, cfg processor.ImageConfig, isBackfill bool, widths []int) ([]processor.ProcessedResult, error) {
		return []processor.ProcessedResult{{Data: []byte("sanitized"), Metadata: &processor.Metadata{}, Kind: processor.KindOriginal}}, nil
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
	err := o.Handle(context.Background(), port.ImageProcessingRequest{
		RawBucket:    "processing",
		RawPath:      "ab/test.jpg",
		OriginalUrl:  "https://example.com/test.jpg",
		TargetBucket: "books",
		TargetPath:   "ab/test.webp",
	})
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

// TestKafkaOrchestrator_Handle_Variants covers the opt-in multi-resolution
// variants feature end to end through the orchestrator: each result's Kind
// must map to its own S3 key suffix (original path untouched, "_wWIDTH" for
// variants), and Kind must be propagated into the completed event so
// downstream consumers can tell a variant from the primary output.
func TestKafkaOrchestrator_Handle_Variants(t *testing.T) {
	testPool := pool.NewWorkerPool(1, processor.DefaultConfig)
	testPool.SetProcessFunc(func(data []byte, cfg processor.ImageConfig, isBackfill bool, widths []int) ([]processor.ProcessedResult, error) {
		return []processor.ProcessedResult{
			{Data: []byte("original"), Metadata: &processor.Metadata{Width: 1200, MimeType: "image/webp"}, Kind: processor.KindOriginal},
			{Data: []byte("v600"), Metadata: &processor.Metadata{Width: 600, MimeType: "image/webp"}, Kind: processor.KindVariant},
			{Data: []byte("v300"), Metadata: &processor.Metadata{Width: 300, MimeType: "image/webp"}, Kind: processor.KindVariant},
		}, nil
	})

	var mu sync.Mutex
	uploaded := map[string][]byte{}
	ms := &mockStorage{
		downloadFunc: func(ctx context.Context, bucket, key string) ([]byte, error) {
			return []byte("raw"), nil
		},
		uploadFunc: func(ctx context.Context, bucket, key string, data []byte, contentType string) error {
			mu.Lock()
			defer mu.Unlock()
			uploaded[key] = data
			return nil
		},
		deleteFunc: func(ctx context.Context, bucket, key string) error {
			return nil
		},
	}

	var emitted []port.ProcessingResult
	mp := &mockProducer{
		emitFunc: func(ctx context.Context, rawPath, originalUrl, targetBucket string, results []port.ProcessingResult) error {
			emitted = results
			return nil
		},
	}

	o := NewKafkaOrchestrator(ms, mp, testPool)
	err := o.Handle(context.Background(), port.ImageProcessingRequest{
		RawBucket:    "processing",
		RawPath:      "ab/test.jpg",
		TargetBucket: "books",
		TargetPath:   "ab/test.webp",
		Widths:       []int{600, 300},
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	wantUploads := map[string]string{
		"ab/test.webp":      "original",
		"ab/test_w600.webp": "v600",
		"ab/test_w300.webp": "v300",
	}
	if len(uploaded) != len(wantUploads) {
		t.Fatalf("expected %d uploaded objects, got %d: %v", len(wantUploads), len(uploaded), uploaded)
	}
	for key, wantData := range wantUploads {
		got, ok := uploaded[key]
		if !ok {
			t.Errorf("expected key %q to be uploaded, it was not (got keys: %v)", key, uploaded)
			continue
		}
		if string(got) != wantData {
			t.Errorf("key %q: expected data %q, got %q", key, wantData, got)
		}
	}

	if len(emitted) != 3 {
		t.Fatalf("expected 3 results in the completed event, got %d", len(emitted))
	}
	wantKinds := map[string]string{
		"ab/test.webp":      processor.KindOriginal,
		"ab/test_w600.webp": processor.KindVariant,
		"ab/test_w300.webp": processor.KindVariant,
	}
	for _, r := range emitted {
		if wantKinds[r.TargetPath] != r.Kind {
			t.Errorf("result %q: expected Kind=%q, got %q", r.TargetPath, wantKinds[r.TargetPath], r.Kind)
		}
	}
}
