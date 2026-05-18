package pool

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/luis/file-processor/internal/processor"
)

func TestNewWorkerPool(t *testing.T) {
	p := NewWorkerPool(0)
	if len(p.jobChan) != runtime.GOMAXPROCS(0) {
		// Note: jobChan capacity is the size
	}
	p.Shutdown()

	p2 := NewWorkerPool(2)
	p2.Shutdown()
}

func TestSubmit_Success(t *testing.T) {
	p := NewWorkerPool(1)
	p.SetProcessFunc(func(data []byte, quality int, isBackfill bool) ([]processor.ProcessedResult, error) {
		return []processor.ProcessedResult{{Data: []byte("processed"), Metadata: &processor.Metadata{}}}, nil
	})
	defer p.Shutdown()

	results, err := p.Submit(context.Background(), []byte("input"), false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 || string(results[0].Data) != "processed" {
		t.Errorf("expected 'processed', got %v", results)
	}
}

func TestSubmit_Timeout(t *testing.T) {
	p := NewWorkerPool(1)
	p.SetProcessFunc(func(data []byte, quality int, isBackfill bool) ([]processor.ProcessedResult, error) {
		time.Sleep(10 * time.Millisecond)
		return []processor.ProcessedResult{{Data: []byte("processed"), Metadata: nil}}, nil
	})
	defer p.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	_, err := p.Submit(ctx, []byte("some data"), false)
	if err != context.DeadlineExceeded {
		t.Errorf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestShutdown(t *testing.T) {
	p := NewWorkerPool(2)
	p.Shutdown()

	_, err := p.Submit(context.Background(), []byte("data"), false)
	if err == nil || err.Error() != "worker pool is closed" {
		t.Errorf("expected 'worker pool is closed' error, got %v", err)
	}
}
