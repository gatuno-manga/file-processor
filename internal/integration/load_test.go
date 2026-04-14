package integration

import (
	"context"
	"encoding/base64"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/luis/file-processor/internal/pool"
	"github.com/luis/file-processor/internal/processor"
)

// A minimal 1x1 transparent GIF image
const testGif = "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"

func TestLoadConcurrency(t *testing.T) {
	// Initialize libvips and pool
	cfg := processor.LoadConfig()
	processor.InitVips(cfg)
	pool.InitPool(cfg.PoolSize)
	defer pool.Shutdown()
	defer processor.ShutdownVips()

	// Decode the base64 image
	input, err := base64.StdEncoding.DecodeString(testGif)
	if err != nil {
		t.Fatalf("Failed to decode base64 GIF: %v", err)
	}

	testCounts := []int{1, 10, 100, 1000}

	for _, count := range testCounts {
		t.Run(fmt.Sprintf("ConcurrentCount-%d", count), func(t *testing.T) {
			var wg sync.WaitGroup
			errChan := make(chan error, count)
			durations := make([]time.Duration, count)
			var mu sync.Mutex
			
			// Capture memory stats before batch
			var memStart runtime.MemStats
			runtime.GC() // Force GC to get a cleaner baseline
			runtime.ReadMemStats(&memStart)
			
			batchStart := time.Now()

			for i := 0; i < count; i++ {
				wg.Add(1)
				index := i
				go func(idx int) {
					defer wg.Done()
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()

					individualStart := time.Now()
					_, err := pool.Submit(ctx, input)
					elapsed := time.Since(individualStart)
					
					if err != nil {
						errChan <- err
					} else {
						mu.Lock()
						durations[idx] = elapsed
						mu.Unlock()
					}
				}(index)
			}

			wg.Wait()
			close(errChan)

			batchDuration := time.Since(batchStart)
			
			// Capture memory stats after batch
			var memEnd runtime.MemStats
			runtime.ReadMemStats(&memEnd)
			
			// Calculate statistics
			var total time.Duration
			var min time.Duration = 999 * time.Hour
			var max time.Duration
			var successCount int

			for _, d := range durations {
				if d > 0 {
					successCount++
					total += d
					if d < min {
						min = d
					}
					if d > max {
						max = d
					}
				}
			}

			t.Logf("--- Batch Summary (%d images) ---", count)
			t.Logf("Total batch time: %v", batchDuration)
			if successCount > 0 {
				t.Logf("Individual Latency - Avg: %v, Min: %v, Max: %v", total/time.Duration(successCount), min, max)
			}
			
			// Resource Usage
			allocMB := float64(memEnd.TotalAlloc-memStart.TotalAlloc) / 1024 / 1024
			heapInUseMB := float64(memEnd.HeapInuse) / 1024 / 1024
			t.Logf("Resource Usage:")
			t.Logf("  - Total RAM Allocated during batch: %.2f MB", allocMB)
			t.Logf("  - Heap In-Use after batch: %.2f MB", heapInUseMB)
			t.Logf("  - GC Cycles during batch: %d", memEnd.NumGC-memStart.NumGC)
			t.Logf("---------------------------------")

			for err := range errChan {
				if err != nil {
					t.Errorf("One of the concurrent processes failed: %v", err)
				}
			}
		})
	}
}
