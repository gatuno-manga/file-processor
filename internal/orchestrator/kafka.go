package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/luis/file-processor/internal/pool"
	"github.com/luis/file-processor/internal/port"
)

// KafkaOrchestrator coordinates the asynchronous image processing flow.
type KafkaOrchestrator struct {
	storage  port.Storage
	producer port.KafkaProducer
	pool     *pool.WorkerPool
}

// NewKafkaOrchestrator creates a new KafkaOrchestrator.
func NewKafkaOrchestrator(storage port.Storage, producer port.KafkaProducer, p *pool.WorkerPool) *KafkaOrchestrator {
	return &KafkaOrchestrator{
		storage:  storage,
		producer: producer,
		pool:     p,
	}
}

// Run starts the orchestration by consuming from the Kafka consumer.
func (o *KafkaOrchestrator) Run(ctx context.Context, consumer port.KafkaConsumer) error {
	slog.Info("Kafka orchestrator starting...")
	return consumer.Consume(ctx, func(ctx context.Context, rawPath, targetBucket, targetPath string, isBackfill bool) error {
		return o.Handle(ctx, rawPath, targetBucket, targetPath, isBackfill)
	})
}

// Handle processes a single image processing request from Kafka.
func (o *KafkaOrchestrator) Handle(ctx context.Context, rawPath, targetBucket, targetPath string, isBackfill bool) error {
	slog.Info("processing image", "rawPath", rawPath, "targetBucket", targetBucket, "targetPath", targetPath, "isBackfill", isBackfill)

	// Sanitize rawPath: remove protocol prefix if present (e.g., https://)
	cleanPath := rawPath
	if idx := strings.Index(cleanPath, "://"); idx != -1 {
		cleanPath = cleanPath[idx+3:]
	}
	// Remove leading slashes
	cleanPath = strings.TrimLeft(cleanPath, "/")

	parts := strings.SplitN(cleanPath, "/", 2)
	if len(parts) < 2 {
		return fmt.Errorf("invalid rawPath format (expected bucket/key): %s", rawPath)
	}
	rawBucket := parts[0]
	rawKey := parts[1]

	// Basic validation for bucket name (no colons, etc.)
	if strings.Contains(rawBucket, ":") || rawBucket == "" {
		return fmt.Errorf("invalid bucket name extracted from path: %s", rawBucket)
	}

	data, err := o.storage.Download(ctx, rawBucket, rawKey)
	if err != nil {
		return fmt.Errorf("failed to download image from s3: %w", err)
	}
	defer o.storage.Release(data)

	processedData, metadata, err := o.pool.Submit(ctx, data, isBackfill)
	if err != nil {
		return fmt.Errorf("failed to process image in pool: %w", err)
	}

	// Optimization for backfill: if image is same (same path and isBackfill),
	// and processedData is same as original data (length-wise check as heuristic, 
	// or we can just rely on the processor returning the original bytes).
	// The requirement says: "Go can opt for only extracting metadata and returning, without re-upload".
	// Since ProcessLossy returns original bytes if isBackfill and already webp.
	if !isBackfill || (targetBucket != rawBucket || targetPath != rawKey) {
		err = o.storage.Upload(ctx, targetBucket, targetPath, processedData)
		if err != nil {
			return fmt.Errorf("failed to upload processed image to s3: %w", err)
		}
	}

	if !isBackfill {
		err = o.storage.Delete(ctx, rawBucket, rawKey)
		if err != nil {
			slog.Warn("failed to delete original image from s3", "error", err, "bucket", rawBucket, "key", rawKey)
		}
	}

	err = o.producer.EmitProcessingCompletedEvent(ctx, rawPath, targetBucket, targetPath, metadata)
	if err != nil {
		return fmt.Errorf("failed to emit processing completed event to kafka: %w", err)
	}

	slog.Info("image processed successfully", "rawPath", rawPath, "targetPath", targetPath)
	return nil
}
