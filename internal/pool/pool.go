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
	results []processor.ProcessedResult
	err     error
}

type WorkerPool struct {
	jobChan     chan job
	processFunc func([]byte, processor.ImageConfig, bool) ([]processor.ProcessedResult, error)
	cfg         processor.ImageConfig
	wg          sync.WaitGroup
	chanPool    sync.Pool
	mu          sync.Mutex
	isClosed    bool
}

func NewWorkerPool(size int, cfg processor.ImageConfig) *WorkerPool {
	if size <= 0 {
		size = runtime.GOMAXPROCS(0)
	}

	p := &WorkerPool{
		jobChan: make(chan job, size),
		cfg:     cfg,
		processFunc: func(data []byte, c processor.ImageConfig, isBackfill bool) ([]processor.ProcessedResult, error) {
			return processor.ProcessLossy(data, c, isBackfill)
		},
		chanPool: sync.Pool{
			New: func() interface{} {
				return make(chan response, 1)
			},
		},
	}

	for i := 0; i < size; i++ {
		p.wg.Add(1)
		go p.worker()
	}

	return p
}

func (p *WorkerPool) SetProcessFunc(f func([]byte, processor.ImageConfig, bool) ([]processor.ProcessedResult, error)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.processFunc = f
}

func (p *WorkerPool) worker() {
	defer p.wg.Done()
	for j := range p.jobChan {
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

			p.mu.Lock()
			f := p.processFunc
			cfg := p.cfg
			p.mu.Unlock()

			res, err := f(j.data, cfg, j.isBackfill)
			j.result <- response{results: res, err: err}
		}()
	}
}

func (p *WorkerPool) Shutdown() {
	p.mu.Lock()
	if p.isClosed {
		p.mu.Unlock()
		return
	}
	p.isClosed = true
	p.mu.Unlock()

	close(p.jobChan)
	p.wg.Wait()
}

func (p *WorkerPool) Submit(ctx context.Context, data []byte, isBackfill bool) ([]processor.ProcessedResult, error) {
	p.mu.Lock()
	if p.isClosed {
		p.mu.Unlock()
		return nil, errors.New("worker pool is closed")
	}
	p.mu.Unlock()

	resChan := p.chanPool.Get().(chan response)
	// NOTE: resChan is deliberately NOT recycled on the cancellation paths below.
	// A worker holding this job may still send into it after we return; recycling
	// would hand a stale response to the next caller. Dropping it is cheap — the
	// pool simply allocates a new one.

	j := job{
		ctx:        ctx,
		data:       data,
		isBackfill: isBackfill,
		result:     resChan,
	}

	select {
	case p.jobChan <- j:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case res := <-resChan:
		p.chanPool.Put(resChan) // safe: buffer is empty and the worker is done
		return res.results, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
