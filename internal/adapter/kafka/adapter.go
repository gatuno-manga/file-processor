package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/luis/file-processor/internal/port"
	"github.com/segmentio/kafka-go"
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

type KafkaAdapter struct {
	writer      kafkaWriter
	reader      kafkaReader
	docWriter   kafkaWriter
	docReader   kafkaReader
	semaphore   chan struct{}
	brokers     []string
	inputTopic  string
	outputTopic string
	docInput    string
	docOutput   string
}

func NewKafkaAdapter(brokers []string, groupID, inputTopic, outputTopic, docInput, docOutput string, maxConcurrentTasks int) *KafkaAdapter {
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

	docWriter := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    docOutput,
		Balancer: &kafka.LeastBytes{},
	}

	docReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       docInput,
		GroupID:     groupID,
		StartOffset: kafka.FirstOffset,
	})

	return &KafkaAdapter{
		writer:      writer,
		reader:      reader,
		docWriter:   docWriter,
		docReader:   docReader,
		semaphore:   make(chan struct{}, maxConcurrentTasks),
		brokers:     brokers,
		inputTopic:  inputTopic,
		outputTopic: outputTopic,
		docInput:    docInput,
		docOutput:   docOutput,
	}
}

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
		Value: payload,
	})
	if err != nil {
		return fmt.Errorf("failed to write message to kafka: %w", err)
	}

	return nil
}

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
		Value: payload,
	})
	if err != nil {
		return fmt.Errorf("failed to write document message to kafka: %w", err)
	}

	return nil
}

func (a *KafkaAdapter) Consume(ctx context.Context, handler func(ctx context.Context, rawBucket, rawPath, originalUrl, targetBucket, targetPath string, isBackfill bool) error) error {
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

			if err := handler(ctx, e.RawBucket, e.RawPath, e.OriginalUrl, e.TargetBucket, e.TargetPath, e.IsBackfill); err != nil {
				slog.Error("failed to handle image processing requested event", "error", err, "rawPath", e.RawPath)
			}

			if err := a.reader.CommitMessages(ctx, m); err != nil {
				slog.Error("failed to commit message to kafka", "error", err)
			}
		}(msg, event)
	}
}

func (a *KafkaAdapter) ConsumeDocumentRequests(ctx context.Context, handler func(ctx context.Context, rawBucket, rawPath, targetBucket, targetPath, format string) error) error {
	defer a.docReader.Close()
	defer a.docWriter.Close()

	for {
		msg, err := a.docReader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("failed to fetch document message from kafka", "error", err)
			continue
		}

		var event DocumentProcessingRequestedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			slog.Error("failed to unmarshal document processing requested event", "error", err)
			a.docReader.CommitMessages(ctx, msg)
			continue
		}

		select {
		case a.semaphore <- struct{}{}:
		case <-ctx.Done():
			return nil
		}

		go func(m kafka.Message, e DocumentProcessingRequestedEvent) {
			defer func() { <-a.semaphore }()

			if err := handler(ctx, e.RawBucket, e.RawPath, e.TargetBucket, e.TargetPath, e.Format); err != nil {
				slog.Error("failed to handle document processing requested event", "error", err, "rawPath", e.RawPath)
			}

			if err := a.docReader.CommitMessages(ctx, m); err != nil {
				slog.Error("failed to commit document message to kafka", "error", err)
			}
		}(msg, event)
	}
}

func (a *KafkaAdapter) IsReady() bool {
	return a.writer != nil && a.reader != nil && a.docWriter != nil && a.docReader != nil
}

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

	client := &kafka.Client{
		Addr:    kafka.TCP(a.brokers...),
		Timeout: 10 * time.Second,
	}

	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{
			{
				Topic:             a.inputTopic,
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
			{
				Topic:             a.outputTopic,
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
			{
				Topic:             a.docInput,
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
			{
				Topic:             a.docOutput,
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
		},
	})

	if err != nil {
		slog.Warn("could not ensure topics exist (they might already exist or broker restricts creation)", "error", err)
	} else {
		for topic, err := range resp.Errors {
			if err != nil && err.Error() != "Topic with this name already exists" {
				slog.Warn("topic creation issue", "topic", topic, "error", err)
			}
		}
	}

	return nil
}

var _ port.KafkaProducer = (*KafkaAdapter)(nil)
var _ port.KafkaConsumer = (*KafkaAdapter)(nil)
