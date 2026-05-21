package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/luis/file-processor/internal/port"
	"github.com/luis/file-processor/internal/processor"
)

type DocumentOrchestrator struct {
	storage  port.Storage
	producer port.KafkaProducer
}

func NewDocumentOrchestrator(storage port.Storage, producer port.KafkaProducer) *DocumentOrchestrator {
	return &DocumentOrchestrator{
		storage:  storage,
		producer: producer,
	}
}

func (o *DocumentOrchestrator) Run(ctx context.Context, consumer port.KafkaConsumer) error {
	slog.Info("Document orchestrator starting...")
	return consumer.ConsumeDocumentRequests(ctx, func(ctx context.Context, rawBucket, rawPath, targetBucket, targetPath, format string) error {
		return o.Handle(ctx, rawBucket, rawPath, targetBucket, targetPath, format)
	})
}

func (o *DocumentOrchestrator) Handle(ctx context.Context, rawBucket, rawPath, targetBucket, targetPath, format string) error {
	slog.Info("processing document", "rawBucket", rawBucket, "rawPath", rawPath, "targetBucket", targetBucket, "targetPath", targetPath, "format", format)

	finalBucket, finalKey, err := parseBucketAndKey(rawBucket, rawPath)
	if err != nil {
		return err
	}

	data, err := o.storage.Download(ctx, finalBucket, finalKey)
	if err != nil {
		return fmt.Errorf("failed to download document from s3: %w", err)
	}
	defer o.storage.Release(data)

	result, err := processor.ProcessDocument(data, format)
	if err != nil {
		return fmt.Errorf("failed to process document: %w", err)
	}

	contentType := "application/pdf"
	if strings.ToUpper(format) == "EPUB" {
		contentType = "application/epub+zip"
	}

	err = o.storage.Upload(ctx, targetBucket, targetPath, result.Data, contentType)
	if err != nil {
		return fmt.Errorf("failed to upload processed document to s3: %w", err)
	}

	err = o.storage.Delete(ctx, finalBucket, finalKey)
	if err != nil {
		slog.Warn("failed to delete original document from s3", "error", err, "bucket", finalBucket, "key", finalKey)
	}

	err = o.producer.EmitDocumentProcessingCompletedEvent(ctx, rawPath, targetBucket, targetPath, &port.DocumentMetadata{
		SizeBytes:    result.SizeBytes,
		PageCount:    result.PageCount,
		IsLinearized: result.IsLinearized,
	})
	if err != nil {
		return fmt.Errorf("failed to emit document completed event to kafka: %w", err)
	}

	slog.Info("document processed successfully", "rawPath", rawPath)
	return nil
}
