package orchestrator

import (
	"context"
	"fmt"
	"log/slog"

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
	return consumer.Consume(ctx, func(ctx context.Context, key, bucket string) error {
		return o.Handle(ctx, key, bucket)
	})
}

// Handle processes a single image processing request from Kafka.
func (o *KafkaOrchestrator) Handle(ctx context.Context, key, bucket string) error {
	slog.Info("processing image", "bucket", bucket, "key", key)

	// 1. Download from S3
	data, err := o.storage.Download(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("failed to download image from s3: %w", err)
	}

	// 2. Submit to worker pool
	sanitizedData, err := pool.Submit(ctx, data)
	if err != nil {
		return fmt.Errorf("failed to process image in pool: %w", err)
	}

	// 3. Upload sanitized WebP to S3
	outputKey := key + ".sanitized.webp"
	err = o.storage.Upload(ctx, bucket, outputKey, sanitizedData)
	if err != nil {
		return fmt.Errorf("failed to upload sanitized image to s3: %w", err)
	}

	// 4. Emit Kafka event
	err = o.producer.EmitSanitizedEvent(ctx, outputKey, bucket)
	if err != nil {
		return fmt.Errorf("failed to emit sanitized event to kafka: %w", err)
	}

	slog.Info("image processed successfully", "bucket", bucket, "key", outputKey)
	return nil
}
