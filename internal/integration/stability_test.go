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
	processor.InitVips(cfg)
	pSize := cfg.PoolSize
	if pSize <= 0 {
		pSize = runtime.GOMAXPROCS(0)
	}
	pool.InitPool(pSize)
	defer pool.Shutdown()
	defer processor.ShutdownVips()

	resolutions := []struct {
		name   string
		width  int
		height int
	}{
		{"800x600", 800, 600},
		{"FullHD", 1920, 1080},
		{"4K", 3840, 2160},
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
					pCtx, pCancel := context.WithTimeout(context.Background(), 120*time.Second)
					defer pCancel()

					start := time.Now()
					_, err := pool.Submit(pCtx, input)
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

			fmt.Printf("\n--- [%s] STABILITY TEST RESULTS (%d images) ---\n", res.name, count)
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
