package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/luis/file-processor/internal/port"
	"github.com/segmentio/kafka-go"
)

// kafkaWriter defines the interface for kafka.Writer methods.
type kafkaWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// kafkaReader defines the interface for kafka.Reader methods.
type kafkaReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// KafkaAdapter implements both port.KafkaProducer and port.KafkaConsumer.
type KafkaAdapter struct {
	writer kafkaWriter
	reader kafkaReader
}

// NewKafkaAdapter creates a new KafkaAdapter.
func NewKafkaAdapter(brokers []string, inputTopic, outputTopic string) *KafkaAdapter {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    outputTopic,
		Balancer: &kafka.LeastBytes{},
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   inputTopic,
		GroupID: "file-processor-group",
	})

	return &KafkaAdapter{
		writer: writer,
		reader: reader,
	}
}

// EmitSanitizedEvent sends a message indicating a file has been sanitized.
func (a *KafkaAdapter) EmitSanitizedEvent(ctx context.Context, key, bucket string) error {
	event := FileSanitizedEvent{
		Bucket: bucket,
		Key:    key,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal sanitized event: %w", err)
	}

	err = a.writer.WriteMessages(ctx, kafka.Message{
		Value: payload,
	})
	if err != nil {
		return fmt.Errorf("failed to write message to kafka: %w", err)
	}

	return nil
}

// Consume starts listening for messages and processes them.
func (a *KafkaAdapter) Consume(ctx context.Context, handler func(ctx context.Context, key, bucket string) error) error {
	defer a.reader.Close()
	defer a.writer.Close()

	for {
		msg, err := a.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("failed to fetch message from kafka", "error", err)
			continue
		}

		var event ImageDownloadedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			slog.Error("failed to unmarshal image downloaded event", "error", err)
			a.reader.CommitMessages(ctx, msg)
			continue
		}

		if err := handler(ctx, event.Key, event.Bucket); err != nil {
			slog.Error("failed to handle image downloaded event", "error", err, "key", event.Key, "bucket", event.Bucket)
			// Depending on policy, we might want to retry or skip.
			// For now, we commit to avoid infinite loop on bad data.
		}

		if err := a.reader.CommitMessages(ctx, msg); err != nil {
			slog.Error("failed to commit message to kafka", "error", err)
		}
	}
}

// IsReady returns true if both the reader and writer are initialized.
func (a *KafkaAdapter) IsReady() bool {
	return a.writer != nil && a.reader != nil
}

// Ensure KafkaAdapter implements the ports.
var _ port.KafkaProducer = (*KafkaAdapter)(nil)
var _ port.KafkaConsumer = (*KafkaAdapter)(nil)
