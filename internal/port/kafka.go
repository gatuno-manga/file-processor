package port

import "context"

// KafkaProducer defines the interface for emitting Kafka events.
type KafkaProducer interface {
	// EmitSanitizedEvent sends a message indicating a file has been sanitized.
	EmitSanitizedEvent(ctx context.Context, key, bucket string) error
}

// KafkaConsumer defines the interface for consuming Kafka events.
type KafkaConsumer interface {
	// Consume starts listening for messages and processes them.
	// It should be a blocking call that terminates when the context is cancelled.
	Consume(ctx context.Context, handler func(ctx context.Context, key, bucket string) error) error
}
