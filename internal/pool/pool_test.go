package pool

import (
	"context"
	"runtime"
	"testing"
	"time"
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
	_, err := Submit(context.Background(), []byte("data"))
	if err == nil || err.Error() != "worker pool not initialized" {
		t.Errorf("expected 'worker pool not initialized' error, got %v", err)
	}
}

func TestSubmit_Success(t *testing.T) {
	ResetPoolForTest()
	// Mock processFunc to return success
	oldProcessFunc := processFunc
	processFunc = func(data []byte) ([]byte, error) {
		return []byte("processed"), nil
	}
	defer func() { processFunc = oldProcessFunc }()

	InitPool(1)

	res, err := Submit(context.Background(), []byte("input"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(res) != "processed" {
		t.Errorf("expected 'processed', got %s", string(res))
	}
}

func TestSubmit_Timeout(t *testing.T) {
	ResetPoolForTest()
	// Mock processFunc to be slow
	oldProcessFunc := processFunc
	processFunc = func(data []byte) ([]byte, error) {
		time.Sleep(10 * time.Millisecond)
		return []byte("processed"), nil
	}
	defer func() { processFunc = oldProcessFunc }()

	InitPool(1)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	_, err := Submit(ctx, []byte("some data"))
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

	_, err := Submit(context.Background(), []byte("data"))
	if err == nil || err.Error() != "worker pool not initialized" {
		t.Errorf("expected 'worker pool not initialized' error, got %v", err)
	}
}
