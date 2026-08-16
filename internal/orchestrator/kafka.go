package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/luis/file-processor/internal/pool"
	"github.com/luis/file-processor/internal/port"
	"github.com/luis/file-processor/internal/processor"
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
	return consumer.Consume(ctx, o.Handle)
}

func (o *KafkaOrchestrator) Handle(ctx context.Context, req port.ImageProcessingRequest) error {
	slog.Info("processing image", "rawBucket", req.RawBucket, "rawPath", req.RawPath, "originalUrl", req.OriginalUrl, "targetBucket", req.TargetBucket, "targetPath", req.TargetPath, "isBackfill", req.IsBackfill, "widths", req.Widths)

	finalBucket, finalKey, err := parseBucketAndKey(req.RawBucket, req.RawPath)
	if err != nil {
		return err
	}

	data, err := o.storage.Download(ctx, finalBucket, finalKey)
	if err != nil {
		return fmt.Errorf("failed to download image from s3: %w", err)
	}
	defer o.storage.Release(data)

	results, err := o.pool.Submit(ctx, data, req.IsBackfill, req.Widths)
	if err != nil {
		return fmt.Errorf("failed to process image in pool: %w", err)
	}

	processingResults := make([]port.ProcessingResult, len(results))
	g, groupCtx := errgroup.WithContext(ctx)

	for i, res := range results {
		i, res := i, res
		g.Go(func() error {
			currentPath := targetPathFor(req.TargetPath, res, i)

			if !req.IsBackfill || (req.TargetBucket != finalBucket || currentPath != finalKey) {
				contentType := "image/webp"
				if res.Metadata != nil && res.Metadata.MimeType != "" {
					contentType = res.Metadata.MimeType
				}

				if err := o.storage.Upload(groupCtx, req.TargetBucket, currentPath, res.Data, contentType); err != nil {
					return fmt.Errorf("failed to upload processed image %q (kind=%s) to s3: %w", currentPath, res.Kind, err)
				}
			}

			processingResults[i] = port.ProcessingResult{
				TargetPath: currentPath,
				Metadata:   res.Metadata,
				Kind:       res.Kind,
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	if !req.IsBackfill {
		if err := o.storage.Delete(ctx, finalBucket, finalKey); err != nil {
			slog.Warn("failed to delete original image from s3", "error", err, "bucket", finalBucket, "key", finalKey)
		}
	}

	if err := o.producer.EmitProcessingCompletedEvent(ctx, req.RawPath, req.OriginalUrl, req.TargetBucket, processingResults); err != nil {
		return fmt.Errorf("failed to emit processing completed event to kafka: %w", err)
	}

	slog.Info("image processed successfully", "rawPath", req.RawPath, "resultsCount", len(results))
	return nil
}

// targetPathFor derives the S3 key for one processed result. "original" keeps
// the caller-requested targetPath untouched; "part" (a tall-image slice) and
// "variant" (a smaller rendition) each get their own suffix convention so the
// two schemes never collide — a request never produces both kinds at once
// (processor.ProcessLossy rejects widths for split-eligible images).
func targetPathFor(targetPath string, res processor.ProcessedResult, index int) string {
	var suffix string
	switch res.Kind {
	case processor.KindPart:
		suffix = fmt.Sprintf("_part%d", index+1)
	case processor.KindVariant:
		width := 0
		if res.Metadata != nil {
			width = res.Metadata.Width
		}
		suffix = fmt.Sprintf("_w%d", width)
	default:
		return targetPath
	}

	extIdx := strings.LastIndex(targetPath, ".")
	if extIdx == -1 {
		return targetPath + suffix
	}
	return targetPath[:extIdx] + suffix + targetPath[extIdx:]
}
