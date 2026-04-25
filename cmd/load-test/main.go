package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"sync"
	"time"

	"github.com/luis/file-processor/internal/pool"
	"github.com/luis/file-processor/internal/processor"
)

func main() {
	inputDir := "/input"
	files, err := ioutil.ReadDir(inputDir)
	if err != nil {
		log.Fatalf("Failed to read input directory: %v", err)
	}

	var targetFile string
	for _, file := range files {
		if !file.IsDir() {
			targetFile = filepath.Join(inputDir, file.Name())
			break
		}
	}

	if targetFile == "" {
		log.Fatal("No files found in input directory")
	}

	data, err := ioutil.ReadFile(targetFile)
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}

	cfg := processor.LoadConfig()
	processor.InitVips(cfg)
	defer processor.ShutdownVips()

	pool.InitPool(cfg.PoolSize)
	defer pool.Shutdown()

	const count = 100
	fmt.Printf("Starting Load Test: 100 iterations of %s\n", filepath.Base(targetFile))
	fmt.Printf("Worker Pool Size: %d\n\n", cfg.PoolSize)

	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, _, err := pool.Submit(context.Background(), data, false)
			if err != nil {
				fmt.Printf("Iteration %d failed: %v\n", id, err)
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	fmt.Printf("\n--- Load Test Results ---\n")
	fmt.Printf("Total Time: %v\n", duration)
	fmt.Printf("Average Time per Image: %v\n", duration/time.Duration(count))
	fmt.Printf("Throughput: %.2f img/sec\n", float64(count)/duration.Seconds())
}
