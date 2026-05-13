package port

import (
	"context"

	"github.com/luis/file-processor/internal/processor"
)

// KafkaProducer defines the interface for emitting Kafka events.
type KafkaProducer interface {
	// EmitProcessingCompletedEvent sends a message indicating an image processing has been completed.
	EmitProcessingCompletedEvent(ctx context.Context, rawPath, targetBucket string, results []ProcessingResult) error

	// EmitDocumentProcessingCompletedEvent sends a message indicating a document processing has been completed.
	EmitDocumentProcessingCompletedEvent(ctx context.Context, rawPath, targetBucket, targetPath string, metadata *DocumentMetadata) error
}

type ProcessingResult struct {
	TargetPath string
	Metadata   *processor.Metadata
}

type DocumentMetadata struct {
	SizeBytes    int
	PageCount    int
	IsLinearized bool
}

// KafkaConsumer defines the interface for consuming Kafka events.
type KafkaConsumer interface {
	// Consume starts listening for messages and processes them.
	// It should be a blocking call that terminates when the context is cancelled.
	Consume(ctx context.Context, handler func(ctx context.Context, rawPath, targetBucket, targetPath string, isBackfill bool) error) error

	// ConsumeDocumentRequests starts listening for document processing requests.
	ConsumeDocumentRequests(ctx context.Context, handler func(ctx context.Context, rawPath, targetBucket, targetPath, format string) error) error
}
