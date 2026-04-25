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
}

// NewKafkaOrchestrator creates a new KafkaOrchestrator.
func NewKafkaOrchestrator(storage port.Storage, producer port.KafkaProducer) *KafkaOrchestrator {
	return &KafkaOrchestrator{
		storage:  storage,
		producer: producer,
	}
}

// Run starts the orchestration by consuming from the Kafka consumer.
func (o *KafkaOrchestrator) Run(ctx context.Context, consumer port.KafkaConsumer) error {
	slog.Info("Kafka orchestrator starting...")
	return consumer.Consume(ctx, func(ctx context.Context, rawPath, targetBucket, targetPath string) error {
		return o.Handle(ctx, rawPath, targetBucket, targetPath)
	})
}

// Handle processes a single image processing request from Kafka.
func (o *KafkaOrchestrator) Handle(ctx context.Context, rawPath, targetBucket, targetPath string) error {
	slog.Info("processing image", "rawPath", rawPath, "targetBucket", targetBucket, "targetPath", targetPath)

	parts := strings.SplitN(rawPath, "/", 2)
	if len(parts) < 2 {
		return fmt.Errorf("invalid rawPath format: %s", rawPath)
	}
	rawBucket := parts[0]
	rawKey := parts[1]

	data, err := o.storage.Download(ctx, rawBucket, rawKey)
	if err != nil {
		return fmt.Errorf("failed to download image from s3: %w", err)
	}

	processedData, err := pool.Submit(ctx, data)
	o.storage.Release(data)
	if err != nil {
		return fmt.Errorf("failed to process image in pool: %w", err)
	}

	err = o.storage.Upload(ctx, targetBucket, targetPath, processedData)
	if err != nil {
		return fmt.Errorf("failed to upload processed image to s3: %w", err)
	}

	err = o.storage.Delete(ctx, rawBucket, rawKey)
	if err != nil {
		slog.Warn("failed to delete original image from s3", "error", err, "bucket", rawBucket, "key", rawKey)
	}

	err = o.producer.EmitProcessingCompletedEvent(ctx, rawPath, targetBucket, targetPath)
	if err != nil {
		return fmt.Errorf("failed to emit processing completed event to kafka: %w", err)
	}

	slog.Info("image processed successfully", "rawPath", rawPath, "targetPath", targetPath)
	return nil
}
