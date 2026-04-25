package pool

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/luis/file-processor/internal/processor"
)

func TestInitPool(t *testing.T) {
	ResetPoolForTest()
	InitPool(0)
	if poolSize != runtime.GOMAXPROCS(0) {
		t.Errorf("expected poolSize to be GOMAXPROCS(%d), got %d", runtime.GOMAXPROCS(0), poolSize)
	}

	ResetPoolForTest()
	InitPool(2)
	if poolSize != 2 {
		t.Errorf("expected poolSize to be 2, got %d", poolSize)
	}
}

func TestSubmit_ErrorNotInitialized(t *testing.T) {
	ResetPoolForTest()
	_, _, err := Submit(context.Background(), []byte("data"), false)
	if err == nil || err.Error() != "worker pool not initialized" {
		t.Errorf("expected 'worker pool not initialized' error, got %v", err)
	}
}

func TestSubmit_Success(t *testing.T) {
	ResetPoolForTest()
	oldProcessFunc := processFunc
	processFunc = func(data []byte, quality int, isBackfill bool) ([]byte, *processor.Metadata, error) {
		return []byte("processed"), &processor.Metadata{}, nil
	}
	defer func() { processFunc = oldProcessFunc }()

	InitPool(1)

	res, _, err := Submit(context.Background(), []byte("input"), false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(res) != "processed" {
		t.Errorf("expected 'processed', got %s", string(res))
	}
}

func TestSubmit_Timeout(t *testing.T) {
	ResetPoolForTest()
	oldProcessFunc := processFunc
	processFunc = func(data []byte, quality int, isBackfill bool) ([]byte, *processor.Metadata, error) {
		time.Sleep(10 * time.Millisecond)
		return []byte("processed"), nil, nil
	}
	defer func() { processFunc = oldProcessFunc }()

	InitPool(1)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	_, _, err := Submit(ctx, []byte("some data"), false)
	if err != context.DeadlineExceeded {
		t.Errorf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestShutdown(t *testing.T) {
	ResetPoolForTest()
	InitPool(2)
	Shutdown()
	if jobChan != nil {
		t.Errorf("expected jobChan to be nil after Shutdown")
	}

	_, _, err := Submit(context.Background(), []byte("data"), false)
	if err == nil || err.Error() != "worker pool not initialized" {
		t.Errorf("expected 'worker pool not initialized' error, got %v", err)
	}
}
