package port

import "context"

// KafkaProducer defines the interface for emitting Kafka events.
type KafkaProducer interface {
	// EmitProcessingCompletedEvent sends a message indicating an image processing has been completed.
	EmitProcessingCompletedEvent(ctx context.Context, rawPath, targetBucket, targetPath string) error
}

// KafkaConsumer defines the interface for consuming Kafka events.
type KafkaConsumer interface {
	// Consume starts listening for messages and processes them.
	// It should be a blocking call that terminates when the context is cancelled.
	Consume(ctx context.Context, handler func(ctx context.Context, rawPath, targetBucket, targetPath string) error) error
}
