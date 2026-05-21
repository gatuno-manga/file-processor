package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/luis/file-processor/internal/port"
	"github.com/segmentio/kafka-go"
)

const (
	initialBackoff = 100 * time.Millisecond
	maxBackoff     = 30 * time.Second
)

type kafkaWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
	Stats() kafka.WriterStats
}

type kafkaReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
	Stats() kafka.ReaderStats
}

// AdapterConfig holds all configuration for topic creation and consumer behaviour.
type AdapterConfig struct {
	Brokers              []string
	GroupID              string
	InputTopic           string
	OutputTopic          string
	DocInput             string
	DocOutput            string
	MaxImageTasks        int
	MaxDocumentTasks     int
	NumPartitions        int
	ReplicationFactor    int
	// StartFromBeginning, when true, sets StartOffset to kafka.FirstOffset.
	// In production this should be false to avoid reprocessing on GroupID changes.
	StartFromBeginning bool
}

// KafkaAdapter implements both KafkaProducer and KafkaConsumer ports.
type KafkaAdapter struct {
	writer            kafkaWriter
	reader            kafkaReader
	docWriter         kafkaWriter
	docReader         kafkaReader
	imageSemaphore    chan struct{}
	documentSemaphore chan struct{}
	brokers           []string
	inputTopic        string
	outputTopic       string
	docInput          string
	docOutput         string
	// healthy tracks whether the last fetch succeeded, used by IsReady.
	healthy atomic.Bool
}

func NewKafkaAdapter(cfg AdapterConfig) *KafkaAdapter {
	startOffset := kafka.LastOffset
	if cfg.StartFromBeginning {
		startOffset = kafka.FirstOffset
	}

	writer := &kafka.Writer{
		Addr:     kafka.TCP(cfg.Brokers...),
		Topic:    cfg.OutputTopic,
		Balancer: &kafka.LeastBytes{},
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		Topic:       cfg.InputTopic,
		GroupID:     cfg.GroupID,
		StartOffset: startOffset,
	})

	docWriter := &kafka.Writer{
		Addr:     kafka.TCP(cfg.Brokers...),
		Topic:    cfg.DocOutput,
		Balancer: &kafka.LeastBytes{},
	}

	docReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		Topic:       cfg.DocInput,
		GroupID:     cfg.GroupID,
		StartOffset: startOffset,
	})

	maxImage := cfg.MaxImageTasks
	if maxImage <= 0 {
		maxImage = 8
	}
	maxDoc := cfg.MaxDocumentTasks
	if maxDoc <= 0 {
		maxDoc = 8
	}

	a := &KafkaAdapter{
		writer:            writer,
		reader:            reader,
		docWriter:         docWriter,
		docReader:         docReader,
		imageSemaphore:    make(chan struct{}, maxImage),
		documentSemaphore: make(chan struct{}, maxDoc),
		brokers:           cfg.Brokers,
		inputTopic:        cfg.InputTopic,
		outputTopic:       cfg.OutputTopic,
		docInput:          cfg.DocInput,
		docOutput:         cfg.DocOutput,
	}
	return a
}

// Close releases all Kafka resources. Call this once when the application shuts down.
func (a *KafkaAdapter) Close() {
	a.writer.Close()
	a.reader.Close()
	a.docWriter.Close()
	a.docReader.Close()
}

// EmitProcessingCompletedEvent publishes an image processing completed event.
// Uses rawPath as the message key to guarantee ordering per file.
func (a *KafkaAdapter) EmitProcessingCompletedEvent(ctx context.Context, rawPath, originalUrl, targetBucket string, results []port.ProcessingResult) error {
	eventResults := make([]ImageProcessingResult, len(results))
	for i, res := range results {
		eventResults[i] = ImageProcessingResult{
			TargetPath: res.TargetPath,
			Metadata: &MetadataEventField{
				Width:         res.Metadata.Width,
				Height:        res.Metadata.Height,
				SizeBytes:     res.Metadata.SizeBytes,
				MimeType:      res.Metadata.MimeType,
				FormatOrigin:  res.Metadata.FormatOrigin,
				BlurHash:      res.Metadata.BlurHash,
				DominantColor: res.Metadata.DominantColor,
				PHash:         res.Metadata.PHash,
				Entropy:       res.Metadata.Entropy,
			},
		}
	}

	event := ImageProcessingCompletedEvent{
		RawPath:      rawPath,
		OriginalUrl:  originalUrl,
		TargetBucket: targetBucket,
		Results:      eventResults,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal processing completed event: %w", err)
	}

	err = a.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(rawPath),
		Value: payload,
	})
	if err != nil {
		return fmt.Errorf("failed to write message to kafka: %w", err)
	}

	return nil
}

// EmitDocumentProcessingCompletedEvent publishes a document processing completed event.
// Uses rawPath as the message key to guarantee ordering per file.
func (a *KafkaAdapter) EmitDocumentProcessingCompletedEvent(ctx context.Context, rawPath, targetBucket, targetPath string, metadata *port.DocumentMetadata) error {
	event := DocumentProcessingCompletedEvent{
		RawPath:      rawPath,
		TargetBucket: targetBucket,
		TargetPath:   targetPath,
		Metadata: &DocumentMetadata{
			SizeBytes:    metadata.SizeBytes,
			PageCount:    metadata.PageCount,
			IsLinearized: metadata.IsLinearized,
		},
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal document processing completed event: %w", err)
	}

	err = a.docWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(rawPath),
		Value: payload,
	})
	if err != nil {
		return fmt.Errorf("failed to write document message to kafka: %w", err)
	}

	return nil
}

// Consume starts the image processing request consumer loop.
// It only commits the message offset after the handler returns successfully.
// On handler failure the message is NOT committed, allowing Kafka to redeliver it.
func (a *KafkaAdapter) Consume(ctx context.Context, handler func(ctx context.Context, rawBucket, rawPath, originalUrl, targetBucket, targetPath string, isBackfill bool) error) error {
	defer a.reader.Close()

	backoff := initialBackoff

	for {
		msg, err := a.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("failed to fetch image message from kafka", "error", err, "retryIn", backoff)
			a.healthy.Store(false)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		// Reset backoff and mark as healthy after a successful fetch.
		backoff = initialBackoff
		a.healthy.Store(true)

		var event ImageProcessingRequestedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			slog.Error("failed to unmarshal image processing requested event", "error", err, "offset", msg.Offset)
			// Malformed message: commit and skip — it will never be processable.
			if err := a.reader.CommitMessages(ctx, msg); err != nil {
				slog.Error("failed to commit malformed image message", "error", err)
			}
			continue
		}

		select {
		case a.imageSemaphore <- struct{}{}:
		case <-ctx.Done():
			return nil
		}

		go func(m kafka.Message, e ImageProcessingRequestedEvent) {
			defer func() { <-a.imageSemaphore }()

			if err := handler(ctx, e.RawBucket, e.RawPath, e.OriginalUrl, e.TargetBucket, e.TargetPath, e.IsBackfill); err != nil {
				// Do NOT commit — Kafka will redeliver the message after consumer restart.
				slog.Error("failed to handle image processing requested event",
					"error", err,
					"rawPath", e.RawPath,
					"rawBucket", e.RawBucket,
				)
				return
			}

			if err := a.reader.CommitMessages(ctx, m); err != nil {
				slog.Error("failed to commit image message to kafka", "error", err)
			}
		}(msg, event)
	}
}

// ConsumeDocumentRequests starts the document processing request consumer loop.
// It only commits the message offset after the handler returns successfully.
// On handler failure the message is NOT committed, allowing Kafka to redeliver it.
func (a *KafkaAdapter) ConsumeDocumentRequests(ctx context.Context, handler func(ctx context.Context, rawBucket, rawPath, targetBucket, targetPath, format string) error) error {
	defer a.docReader.Close()

	backoff := initialBackoff

	for {
		msg, err := a.docReader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("failed to fetch document message from kafka", "error", err, "retryIn", backoff)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		backoff = initialBackoff

		var event DocumentProcessingRequestedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			slog.Error("failed to unmarshal document processing requested event", "error", err, "offset", msg.Offset)
			// Malformed message: commit and skip.
			if err := a.docReader.CommitMessages(ctx, msg); err != nil {
				slog.Error("failed to commit malformed document message", "error", err)
			}
			continue
		}

		select {
		case a.documentSemaphore <- struct{}{}:
		case <-ctx.Done():
			return nil
		}

		go func(m kafka.Message, e DocumentProcessingRequestedEvent) {
			defer func() { <-a.documentSemaphore }()

			if err := handler(ctx, e.RawBucket, e.RawPath, e.TargetBucket, e.TargetPath, e.Format); err != nil {
				// Do NOT commit — Kafka will redeliver the message after consumer restart.
				slog.Error("failed to handle document processing requested event",
					"error", err,
					"rawPath", e.RawPath,
					"rawBucket", e.RawBucket,
				)
				return
			}

			if err := a.docReader.CommitMessages(ctx, m); err != nil {
				slog.Error("failed to commit document message to kafka", "error", err)
			}
		}(msg, event)
	}
}

// IsReady returns true if the adapter has successfully fetched at least one message
// since startup and is not in an error backoff state.
func (a *KafkaAdapter) IsReady() bool {
	return a.writer != nil && a.reader != nil && a.docWriter != nil && a.docReader != nil && a.healthy.Load()
}

// Ping verifies connectivity to all Kafka brokers. Topic creation, if needed,
// should be handled by infrastructure tooling (Terraform, Helm, etc.), not the worker.
func (a *KafkaAdapter) Ping(ctx context.Context) error {
	if len(a.brokers) == 0 {
		return nil
	}

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	for _, broker := range a.brokers {
		conn, err := dialer.DialContext(ctx, "tcp", broker)
		if err != nil {
			return fmt.Errorf("failed to connect to kafka broker %s: %w", broker, err)
		}
		conn.Close()
	}

	return nil
}

// min returns the smaller of two durations.
func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

var _ port.KafkaProducer = (*KafkaAdapter)(nil)
var _ port.KafkaConsumer = (*KafkaAdapter)(nil)
