package integration

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/h2non/bimg"
	"github.com/luis/file-processor/internal/pool"
	"github.com/luis/file-processor/internal/processor"
)

const minimalGif = "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"

func TestMain(m *testing.M) {
	cfg := processor.LoadConfig()
	processor.InitVips(cfg)
	code := m.Run()
	processor.ShutdownVips()
	os.Exit(code)
}

func generateTestImage(width, height int) ([]byte, error) {
	input, err := base64.StdEncoding.DecodeString(minimalGif)
	if err != nil {
		return nil, err
	}
	if width <= 1 && height <= 1 {
		return input, nil
	}
	return bimg.NewImage(input).Process(bimg.Options{
		Width:  width,
		Height: height,
		Type:   bimg.JPEG,
	})
}

func TestLoadConcurrency(t *testing.T) {
	cfg := processor.LoadConfig()
	pool.InitPool(cfg.PoolSize)
	defer pool.Shutdown()

	resolutions := []struct {
		name   string
		width  int
		height int
	}{
		{"1x1", 1, 1},
		{"FullHD", 1920, 1080},
		{"4K", 3840, 2160},
	}

	testCounts := []int{1, 10, 100, 1000}

	for _, res := range resolutions {
		t.Run(fmt.Sprintf("Resolution-%s", res.name), func(t *testing.T) {
			input, err := generateTestImage(res.width, res.height)
			if err != nil {
				t.Fatalf("Failed to generate %s image: %v", res.name, err)
			}
			t.Logf("Generated %s image (Size: %.2f KB)", res.name, float64(len(input))/1024)

			for _, count := range testCounts {
				if res.name == "4K" && count > 100 {
					continue 
				}

				t.Run(fmt.Sprintf("ConcurrentCount-%d", count), func(t *testing.T) {
					var wg sync.WaitGroup
					errChan := make(chan error, count)
					durations := make([]time.Duration, count)
					var mu sync.Mutex
					
					var memStart runtime.MemStats
					runtime.GC()
					runtime.ReadMemStats(&memStart)
					
					batchStart := time.Now()

					for i := 0; i < count; i++ {
						wg.Add(1)
						index := i
						go func(idx int) {
							defer wg.Done()
							ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
							defer cancel()

							individualStart := time.Now()
							_, _, err := pool.Submit(ctx, input, false)
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
					var memEnd runtime.MemStats
					runtime.ReadMemStats(&memEnd)
					
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

					t.Logf("--- Batch Summary [%s | %d images] ---", res.name, count)
					t.Logf("Total batch time: %v", batchDuration)
					if successCount > 0 {
						t.Logf("Individual Latency - Avg: %v, Min: %v, Max: %v", total/time.Duration(successCount), min, max)
					}
					
					allocMB := float64(memEnd.TotalAlloc-memStart.TotalAlloc) / 1024 / 1024
					heapInUseMB := float64(memEnd.HeapInuse) / 1024 / 1024
					t.Logf("Resource Usage:")
					t.Logf("  - RAM Allocated: %.2f MB", allocMB)
					t.Logf("  - Heap In-Use: %.2f MB", heapInUseMB)
					t.Logf("---------------------------------")

					for err := range errChan {
						if err != nil {
							t.Errorf("Process failed: %v", err)
						}
					}
				})
			}
		})
	}
}
