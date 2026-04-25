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
	writer    kafkaWriter
	reader    kafkaReader
	semaphore chan struct{}
}

// NewKafkaAdapter creates a new KafkaAdapter.
func NewKafkaAdapter(brokers []string, groupID, inputTopic, outputTopic string, maxConcurrentTasks int) *KafkaAdapter {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    outputTopic,
		Balancer: &kafka.LeastBytes{},
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       inputTopic,
		GroupID:     groupID,
		StartOffset: kafka.FirstOffset,
	})

	return &KafkaAdapter{
		writer:    writer,
		reader:    reader,
		semaphore: make(chan struct{}, maxConcurrentTasks),
	}
}

// EmitProcessingCompletedEvent sends a message indicating image processing is complete.
func (a *KafkaAdapter) EmitProcessingCompletedEvent(ctx context.Context, rawPath, targetBucket, targetPath string) error {
	event := ImageProcessingCompletedEvent{
		RawPath:      rawPath,
		TargetBucket: targetBucket,
		TargetPath:   targetPath,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal processing completed event: %w", err)
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
func (a *KafkaAdapter) Consume(ctx context.Context, handler func(ctx context.Context, rawPath, targetBucket, targetPath string) error) error {
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

		var event ImageProcessingRequestedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			slog.Error("failed to unmarshal image processing requested event", "error", err)
			a.reader.CommitMessages(ctx, msg)
			continue
		}

		select {
		case a.semaphore <- struct{}{}:
		case <-ctx.Done():
			return nil
		}

		go func(m kafka.Message, e ImageProcessingRequestedEvent) {
			defer func() { <-a.semaphore }()

			if err := handler(ctx, e.RawPath, e.TargetBucket, e.TargetPath); err != nil {
				slog.Error("failed to handle image processing requested event", "error", err, "rawPath", e.RawPath)
			}

			if err := a.reader.CommitMessages(ctx, m); err != nil {
				slog.Error("failed to commit message to kafka", "error", err)
			}
		}(msg, event)
	}
}

// IsReady returns true if both the reader and writer are initialized.
func (a *KafkaAdapter) IsReady() bool {
	return a.writer != nil && a.reader != nil
}

var _ port.KafkaProducer = (*KafkaAdapter)(nil)
var _ port.KafkaConsumer = (*KafkaAdapter)(nil)
