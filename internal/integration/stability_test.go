package integration

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/h2non/bimg"
	"github.com/luis/file-processor/internal/pool"
	"github.com/luis/file-processor/internal/processor"
)

const minimalGifBase64 = "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"

func generateStabilityImage(width, height int) ([]byte, error) {
	input, err := base64.StdEncoding.DecodeString(minimalGifBase64)
	if err != nil {
		return nil, err
	}
	return bimg.NewImage(input).Process(bimg.Options{
		Width:  width,
		Height: height,
		Type:   bimg.JPEG,
	})
}

type Metrics struct {
	PeakRSS float64
	PeakCPU float64
}

func getRSS() float64 {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return 0
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "VmRSS:") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				val, _ := strconv.ParseFloat(parts[1], 64)
				return val / 1024.0
			}
		}
	}
	return 0
}

func getCPUTicks() int64 {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 15 {
		return 0
	}
	utime, _ := strconv.ParseInt(fields[13], 10, 64)
	stime, _ := strconv.ParseInt(fields[14], 10, 64)
	return utime + stime
}

func monitorResources(ctx context.Context, wg *sync.WaitGroup, m *Metrics) {
	defer wg.Done()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	lastTicks := getCPUTicks()
	lastTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			rss := getRSS()
			if rss > m.PeakRSS {
				m.PeakRSS = rss
			}

			currentTicks := getCPUTicks()
			deltaTicks := currentTicks - lastTicks
			deltaTime := now.Sub(lastTime).Seconds()

			if deltaTime > 0 {
				cpuPercent := float64(deltaTicks) / deltaTime
				if cpuPercent > m.PeakCPU {
					m.PeakCPU = cpuPercent
				}
			}
			lastTicks = currentTicks
			lastTime = now
		}
	}
}

func TestStabilityBenchmark(t *testing.T) {
	cfg := processor.LoadConfig()
	pSize := cfg.PoolSize
	if pSize <= 0 {
		pSize = runtime.GOMAXPROCS(0)
	}
	p := pool.NewWorkerPool(pSize, processor.DefaultConfig)
	defer p.Shutdown()

	resolutions := []struct {
		name   string
		width  int
		height int
		// widths requests the opt-in multi-resolution variants feature
		// alongside the primary output, to measure its real RSS/CPU/latency
		// cost against the equivalent no-variants baseline at the same
		// resolution and concurrency (see .planning/research/IMPACT-multi-resolution-variants.md §3).
		widths []int
	}{
		{"800x600", 800, 600, nil},
		{"FullHD", 1920, 1080, nil},
		{"FullHD+variants(600,300)", 1920, 1080, []int{600, 300}},
		{"4K", 3840, 2160, nil},
	}

	const count = 1000

	fmt.Printf("\nEnvironment: Go %s, GOMAXPROCS: %d, PoolSize: %d\n", runtime.Version(), runtime.GOMAXPROCS(0), pSize)

	for _, res := range resolutions {
		t.Run(res.name, func(t *testing.T) {
			input, err := generateStabilityImage(res.width, res.height)
			if err != nil {
				t.Fatalf("Failed to generate %s image: %v", res.name, err)
			}

			metrics := &Metrics{}
			ctx, cancel := context.WithCancel(context.Background())
			var monitorWg sync.WaitGroup
			monitorWg.Add(1)
			go monitorResources(ctx, &monitorWg, metrics)

			var wg sync.WaitGroup
			errChan := make(chan error, count)
			durations := make([]time.Duration, count)
			var mu sync.Mutex

			batchStart := time.Now()
			for i := 0; i < count; i++ {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					pCtx, pCancel := context.WithTimeout(context.Background(), 300*time.Second)
					defer pCancel()

					start := time.Now()
					_, err := p.Submit(pCtx, input, false, res.widths)
					elapsed := time.Since(start)

					if err != nil {
						errChan <- fmt.Errorf("image %d: %v", idx, err)
					} else {
						mu.Lock()
						durations[idx] = elapsed
						mu.Unlock()
					}
				}(i)
			}

			wg.Wait()
			batchDuration := time.Since(batchStart)
			cancel()
			monitorWg.Wait()
			close(errChan)

			var totalLat time.Duration
			var successCount int
			for _, d := range durations {
				if d > 0 {
					successCount++
					totalLat += d
				}
			}

			fmt.Printf("\n--- [%s] STABILITY TEST RESULTS (%d images, widths=%v) ---\n", res.name, count, res.widths)
			fmt.Printf("Tempo Total:       %v\n", batchDuration)
			if successCount > 0 {
				fmt.Printf("Tempo por Imagem:  %.4f ms (Avg Latency)\n", float64(totalLat.Milliseconds())/float64(successCount))
			}
			fmt.Printf("Pico de RAM (RSS): %.2f MB\n", metrics.PeakRSS)
			fmt.Printf("Pico de CPU:       %.2f %%\n", metrics.PeakCPU)
			fmt.Printf("Sucesso:           %d/%d\n", successCount, count)
			fmt.Printf("Throughput:        %.2f img/sec\n", float64(successCount)/batchDuration.Seconds())
			fmt.Printf("---------------------------------------------\n")

			for err := range errChan {
				t.Errorf("Process error: %v", err)
			}
		})
	}
}

// TestStabilityBenchmark_SingleImageLatency measures per-image processing
// time with no queueing/contention: one image submitted at a time, sequentially,
// against a single-worker pool. TestStabilityBenchmark's "Tempo por Imagem"
// figure is measured under 1000-way concurrency against a 4-worker pool, so it
// includes queue wait time and is not a per-image service-time figure. This
// test isolates the actual libvips decode+encode(+variants) cost per call.
func TestStabilityBenchmark_SingleImageLatency(t *testing.T) {
	p := pool.NewWorkerPool(1, processor.DefaultConfig)
	defer p.Shutdown()

	const iterations = 30
	const warmup = 3

	cases := []struct {
		name   string
		widths []int
	}{
		{"FullHD", nil},
		{"FullHD+variants(600,300)", []int{600, 300}},
	}

	input, err := generateStabilityImage(1920, 1080)
	if err != nil {
		t.Fatalf("failed to generate FullHD fixture: %v", err)
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var durations []time.Duration
			for i := 0; i < warmup+iterations; i++ {
				start := time.Now()
				if _, err := p.Submit(context.Background(), input, false, c.widths); err != nil {
					t.Fatalf("Submit failed: %v", err)
				}
				elapsed := time.Since(start)
				if i >= warmup {
					durations = append(durations, elapsed)
				}
			}

			var total, min, max time.Duration
			min = durations[0]
			for _, d := range durations {
				total += d
				if d < min {
					min = d
				}
				if d > max {
					max = d
				}
			}
			avg := total / time.Duration(len(durations))

			fmt.Printf("\n--- [%s] SINGLE-IMAGE LATENCY (%d samples, widths=%v) ---\n", c.name, len(durations), c.widths)
			fmt.Printf("Min:  %v\n", min)
			fmt.Printf("Avg:  %v\n", avg)
			fmt.Printf("Max:  %v\n", max)
			fmt.Printf("---------------------------------------------\n")
		})
	}
}
