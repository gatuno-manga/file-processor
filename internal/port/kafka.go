package port

import (
	"context"

	"github.com/luis/file-processor/internal/processor"
)

// KafkaProducer defines the interface for emitting Kafka events.
type KafkaProducer interface {
	// EmitProcessingCompletedEvent sends a message indicating an image processing has been completed.
	EmitProcessingCompletedEvent(ctx context.Context, rawPath, originalUrl, targetBucket string, results []ProcessingResult) error

	// EmitDocumentProcessingCompletedEvent sends a message indicating a document processing has been completed.
	EmitDocumentProcessingCompletedEvent(ctx context.Context, rawPath, targetBucket, targetPath string, metadata *DocumentMetadata) error
}

type ProcessingResult struct {
	TargetPath string
	Metadata   *processor.Metadata
	// Kind is one of processor.KindOriginal, processor.KindPart or
	// processor.KindVariant — see internal/processor.ProcessedResult.
	Kind string
}

type DocumentMetadata struct {
	SizeBytes    int
	PageCount    int
	IsLinearized bool
}

// ImageProcessingRequest carries every field needed to handle one image
// processing job. Grouped into a struct — rather than growing the handler's
// positional parameter list again — per the audit's cross-cutting note on
// port.KafkaConsumer signature churn (.planning/audit/README.md §5.2).
type ImageProcessingRequest struct {
	RawBucket    string
	RawPath      string
	OriginalUrl  string
	TargetBucket string
	TargetPath   string
	IsBackfill   bool
	// Widths is an optional, caller-supplied list of additional smaller
	// renditions to generate alongside the primary output. Empty means no
	// variants: this feature is opt-in per request, not automatic.
	Widths []int
}

// KafkaConsumer defines the interface for consuming Kafka events.
type KafkaConsumer interface {
	// Consume starts listening for messages and processes them.
	// It should be a blocking call that terminates when the context is cancelled.
	Consume(ctx context.Context, handler func(ctx context.Context, req ImageProcessingRequest) error) error

	// ConsumeDocumentRequests starts listening for document processing requests.
	ConsumeDocumentRequests(ctx context.Context, handler func(ctx context.Context, rawBucket, rawPath, targetBucket, targetPath, format string) error) error
}
