package pool

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/luis/file-processor/internal/processor"
)

func TestNewWorkerPool(t *testing.T) {
	p := NewWorkerPool(0, processor.DefaultConfig)
	if len(p.jobChan) != runtime.GOMAXPROCS(0) {
		// Note: jobChan capacity is the size
	}
	p.Shutdown()

	p2 := NewWorkerPool(2, processor.DefaultConfig)
	p2.Shutdown()
}

func TestSubmit_Success(t *testing.T) {
	p := NewWorkerPool(1, processor.DefaultConfig)
	p.SetProcessFunc(func(data []byte, cfg processor.ImageConfig, isBackfill bool) ([]processor.ProcessedResult, error) {
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
	p := NewWorkerPool(1, processor.DefaultConfig)
	p.SetProcessFunc(func(data []byte, cfg processor.ImageConfig, isBackfill bool) ([]processor.ProcessedResult, error) {
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

// TestWorkerPool_CancelledSubmitDoesNotLeakResult guards against a
// use-after-recycle bug: if Submit unconditionally returns resChan to
// chanPool on a cancellation path, a worker that is still in flight can
// later write its result into that recycled channel, and the next caller
// to draw it from the pool receives a stale result belonging to someone
// else's request (see audit finding C2).
func TestWorkerPool_CancelledSubmitDoesNotLeakResult(t *testing.T) {
	p := NewWorkerPool(1, processor.DefaultConfig)
	unblockA := make(chan struct{})
	sentA := make(chan struct{})
	p.SetProcessFunc(func(data []byte, cfg processor.ImageConfig, isBackfill bool) ([]processor.ProcessedResult, error) {
		if string(data) == "A" {
			<-unblockA
			close(sentA)
			return []processor.ProcessedResult{{Data: []byte("result-A")}}, nil
		}
		return []processor.ProcessedResult{{Data: []byte("result-B")}}, nil
	})
	defer p.Shutdown()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// Give Submit time to enqueue job A and start waiting on resChan
		// before cancelling, so it returns via the ctx.Done() branch while
		// the worker is still processing A.
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	_, err := p.Submit(ctx, []byte("A"), false)
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// Let the worker finish processing A and write its now-orphaned result.
	close(unblockA)
	<-sentA
	time.Sleep(15 * time.Millisecond)

	results, err := p.Submit(context.Background(), []byte("B"), false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 || string(results[0].Data) != "result-B" {
		t.Fatalf("expected result-B, got %v (leaked another request's result)", results)
	}
}

func TestShutdown(t *testing.T) {
	p := NewWorkerPool(2, processor.DefaultConfig)
	p.Shutdown()

	_, err := p.Submit(context.Background(), []byte("data"), false)
	if err == nil || err.Error() != "worker pool is closed" {
		t.Errorf("expected 'worker pool is closed' error, got %v", err)
	}
}
