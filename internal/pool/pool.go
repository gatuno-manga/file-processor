package pool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"

	"github.com/luis/file-processor/internal/processor"
)

type job struct {
	ctx        context.Context
	data       []byte
	isBackfill bool
	result     chan response
}

type response struct {
	data     []byte
	metadata *processor.Metadata
	err      error
}

var (
	jobChan     chan job
	poolSize    int
	processFunc = func(data []byte, quality int, isBackfill bool) ([]byte, *processor.Metadata, error) {
		return processor.ProcessLossy(data, quality, isBackfill)
	}
	wg sync.WaitGroup
	mu sync.Mutex

	chanPool = sync.Pool{
		New: func() interface{} {
			return make(chan response, 1)
		},
	}
)

// SetProcessFunc allows overriding the processing logic, mainly for testing.
func SetProcessFunc(f func([]byte, int, bool) ([]byte, *processor.Metadata, error)) {
	processFunc = f
}

// InitPool initializes the worker pool with the given size.
// If size is <= 0, it defaults to runtime.GOMAXPROCS(0).
func InitPool(size int) {
	mu.Lock()
	defer mu.Unlock()

	if jobChan != nil {
		return
	}

	if size <= 0 {
		size = runtime.GOMAXPROCS(0)
	}
	poolSize = size
	jobChan = make(chan job, size)

	for i := 0; i < size; i++ {
		wg.Add(1)
		go worker(jobChan)
	}
}

func worker(ch chan job) {
	defer wg.Done()
	for j := range ch {
		func() {
			defer func() {
				if r := recover(); r != nil {
					slog.Error("worker panicked while processing image", "panic", r)
					j.result <- response{err: fmt.Errorf("worker panic: %v", r)}
				}
			}()

			select {
			case <-j.ctx.Done():
				j.result <- response{err: j.ctx.Err()}
				return
			default:
			}

			res, meta, err := processFunc(j.data, processor.DefaultQuality, j.isBackfill)
			j.result <- response{data: res, metadata: meta, err: err}
		}()
	}
}

// Shutdown closes the job channel and waits for all workers to finish.
func Shutdown() {
	mu.Lock()
	if jobChan == nil {
		mu.Unlock()
		return
	}

	ch := jobChan
	jobChan = nil
	mu.Unlock()

	close(ch)
	wg.Wait()
}

// IsReady returns true if the worker pool is initialized and active.
func IsReady() bool {
	mu.Lock()
	defer mu.Unlock()
	return jobChan != nil
}

// ResetPoolForTest shuts down the current pool and resets state for testing.
func ResetPoolForTest() {
	Shutdown()
}

// Submit sends a job to the worker pool and blocks until completion or context expiration.
func Submit(ctx context.Context, data []byte, isBackfill bool) ([]byte, *processor.Metadata, error) {
	mu.Lock()
	ch := jobChan
	mu.Unlock()

	if ch == nil {
		return nil, nil, errors.New("worker pool not initialized")
	}

	resChan := chanPool.Get().(chan response)
	defer chanPool.Put(resChan)

	j := job{
		ctx:        ctx,
		data:       data,
		isBackfill: isBackfill,
		result:     resChan,
	}

	select {
	case ch <- j:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}

	select {
	case res := <-resChan:
		return res.data, res.metadata, res.err
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}
