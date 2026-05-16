package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/luis/file-processor/internal/pool"
	"github.com/luis/file-processor/internal/port"
	"golang.org/x/sync/errgroup"
)

type KafkaOrchestrator struct {
	storage  port.Storage
	producer port.KafkaProducer
	pool     *pool.WorkerPool
}

func NewKafkaOrchestrator(storage port.Storage, producer port.KafkaProducer, p *pool.WorkerPool) *KafkaOrchestrator {
	return &KafkaOrchestrator{
		storage:  storage,
		producer: producer,
		pool:     p,
	}
}

func (o *KafkaOrchestrator) Run(ctx context.Context, consumer port.KafkaConsumer) error {
	slog.Info("Kafka orchestrator starting...")
	return consumer.Consume(ctx, func(ctx context.Context, rawBucket, rawPath, originalUrl, targetBucket, targetPath string, isBackfill bool) error {
		return o.Handle(ctx, rawBucket, rawPath, originalUrl, targetBucket, targetPath, isBackfill)
	})
}

func (o *KafkaOrchestrator) Handle(ctx context.Context, rawBucket, rawPath, originalUrl, targetBucket, targetPath string, isBackfill bool) error {
	slog.Info("processing image", "rawBucket", rawBucket, "rawPath", rawPath, "originalUrl", originalUrl, "targetBucket", targetBucket, "targetPath", targetPath, "isBackfill", isBackfill)

	cleanPath := rawPath
	if idx := strings.Index(cleanPath, "://"); idx != -1 {
		cleanPath = cleanPath[idx+3:]
	}
	cleanPath = strings.TrimLeft(cleanPath, "/")

	finalBucket := rawBucket
	finalKey := cleanPath

	// If bucket is explicitly provided, and the path starts with "bucket/", trim it
	// to avoid redundant paths like bucket/bucket/key
	if finalBucket != "" && strings.HasPrefix(finalKey, finalBucket+"/") {
		finalKey = finalKey[len(finalBucket)+1:]
	}

	// Fallback for backward compatibility where rawPath might be "bucket/key"
	if finalBucket == "" {
		parts := strings.SplitN(cleanPath, "/", 2)
		if len(parts) < 2 {
			return fmt.Errorf("invalid rawPath format (expected bucket/key): %s", rawPath)
		}
		finalBucket = parts[0]
		finalKey = parts[1]
	}

	if strings.Contains(finalBucket, ":") || finalBucket == "" {
		return fmt.Errorf("invalid bucket name: %s", finalBucket)
	}

	data, err := o.storage.Download(ctx, finalBucket, finalKey)
	if err != nil {
		return fmt.Errorf("failed to download image from s3: %w", err)
	}
	defer o.storage.Release(data)

	results, err := o.pool.Submit(ctx, data, isBackfill)
	if err != nil {
		return fmt.Errorf("failed to process image in pool: %w", err)
	}

	processingResults := make([]port.ProcessingResult, len(results))
	g, groupCtx := errgroup.WithContext(ctx)

	for i, res := range results {
		i, res := i, res
		g.Go(func() error {
			currentPath := targetPath
			if len(results) > 1 {
				extIdx := strings.LastIndex(targetPath, ".")
				if extIdx != -1 {
					currentPath = fmt.Sprintf("%s_part%d%s", targetPath[:extIdx], i+1, targetPath[extIdx:])
				} else {
					currentPath = fmt.Sprintf("%s_part%d", targetPath, i+1)
				}
			}

			if !isBackfill || (targetBucket != finalBucket || currentPath != finalKey) {
				contentType := "image/webp"
				if res.Metadata != nil && res.Metadata.MimeType != "" {
					contentType = res.Metadata.MimeType
				}

				if err := o.storage.Upload(groupCtx, targetBucket, currentPath, res.Data, contentType); err != nil {
					return fmt.Errorf("failed to upload processed image part %d to s3: %w", i+1, err)
				}
			}

			processingResults[i] = port.ProcessingResult{
				TargetPath: currentPath,
				Metadata:   res.Metadata,
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	if !isBackfill {
		if err := o.storage.Delete(ctx, finalBucket, finalKey); err != nil {
			slog.Warn("failed to delete original image from s3", "error", err, "bucket", finalBucket, "key", finalKey)
		}
	}

	if err := o.producer.EmitProcessingCompletedEvent(ctx, rawPath, originalUrl, targetBucket, processingResults); err != nil {
		return fmt.Errorf("failed to emit processing completed event to kafka: %w", err)
	}

	slog.Info("image processed successfully", "rawPath", rawPath, "resultsCount", len(results))
	return nil
}
