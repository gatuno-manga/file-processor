package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/luis/file-processor/internal/port"
	"github.com/luis/file-processor/internal/processor"
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
	Brokers           []string
	GroupID           string
	InputTopic        string
	OutputTopic       string
	DocInput          string
	DocOutput         string
	MaxImageTasks     int
	MaxDocumentTasks  int
	NumPartitions     int
	ReplicationFactor int
	// StartFromBeginning, when true, sets StartOffset to kafka.FirstOffset.
	// In production this should be false to avoid reprocessing on GroupID changes.
	StartFromBeginning bool
	// DLQSuffix is appended to the input topic name to derive the dead-letter topic.
	// Defaults to ".dlq" if empty.
	DLQSuffix string
	// MaxDeliveryTries caps in-message retries before a message is routed to the DLQ.
	// Defaults to 3 if <= 0.
	MaxDeliveryTries int
	// RetryBackoff is the initial delay between retries; it doubles after each attempt.
	// Defaults to 500ms if <= 0.
	RetryBackoff time.Duration
	// ProcessTimeout bounds how long a single message handler may run.
	// Defaults to 120s if <= 0.
	ProcessTimeout time.Duration
}

// KafkaAdapter implements both KafkaProducer and KafkaConsumer ports.
type KafkaAdapter struct {
	writer            kafkaWriter
	reader            kafkaReader
	docWriter         kafkaWriter
	docReader         kafkaReader
	dlqWriter         kafkaWriter
	docDlqWriter      kafkaWriter
	imageSemaphore    chan struct{}
	documentSemaphore chan struct{}
	brokers           []string
	inputTopic        string
	outputTopic       string
	docInput          string
	docOutput         string
	dlqSuffix         string
	maxTries          int
	retryBackoff      time.Duration
	processTimeout    time.Duration
	// healthy tracks whether the last fetch succeeded, used by IsReady.
	healthy atomic.Bool
}

func NewKafkaAdapter(cfg AdapterConfig) *KafkaAdapter {
	startOffset := resolveStartOffset(cfg.StartFromBeginning)
	slog.Info("kafka reader offset policy",
		"startFromBeginning", cfg.StartFromBeginning,
		"groupID", cfg.GroupID,
		"resolvedOffset", startOffset,
	)

	writer := &kafka.Writer{
		Addr:     kafka.TCP(cfg.Brokers...),
		Topic:    cfg.OutputTopic,
		Balancer: &kafka.LeastBytes{},
	}

	// 3. GroupID Isolado: Mantemos o ID original para imagens para preservar o offset/histórico
	// e criamos um novo (-docs) para isolar os documentos.
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

	// 3. GroupID Isolado: Adicionado sufixo para documentos
	docReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		Topic:       cfg.DocInput,
		GroupID:     cfg.GroupID + "-docs",
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

	dlqSuffix := cfg.DLQSuffix
	if dlqSuffix == "" {
		dlqSuffix = ".dlq"
	}
	maxTries := cfg.MaxDeliveryTries
	if maxTries <= 0 {
		maxTries = 3
	}
	retryBackoff := cfg.RetryBackoff
	if retryBackoff <= 0 {
		retryBackoff = 500 * time.Millisecond
	}
	processTimeout := cfg.ProcessTimeout
	if processTimeout <= 0 {
		processTimeout = 120 * time.Second
	}

	dlqWriter := &kafka.Writer{
		Addr:     kafka.TCP(cfg.Brokers...),
		Topic:    cfg.InputTopic + dlqSuffix,
		Balancer: &kafka.LeastBytes{},
	}
	docDlqWriter := &kafka.Writer{
		Addr:     kafka.TCP(cfg.Brokers...),
		Topic:    cfg.DocInput + dlqSuffix,
		Balancer: &kafka.LeastBytes{},
	}

	a := &KafkaAdapter{
		writer:            writer,
		reader:            reader,
		docWriter:         docWriter,
		docReader:         docReader,
		dlqWriter:         dlqWriter,
		docDlqWriter:      docDlqWriter,
		imageSemaphore:    make(chan struct{}, maxImage),
		documentSemaphore: make(chan struct{}, maxDoc),
		brokers:           cfg.Brokers,
		inputTopic:        cfg.InputTopic,
		outputTopic:       cfg.OutputTopic,
		docInput:          cfg.DocInput,
		docOutput:         cfg.DocOutput,
		dlqSuffix:         dlqSuffix,
		maxTries:          maxTries,
		retryBackoff:      retryBackoff,
		processTimeout:    processTimeout,
	}
	a.healthy.Store(true)
	return a
}

// resolveStartOffset returns the Kafka consumer start-offset policy.
// LastOffset (skip backlog) is the safe default; FirstOffset (replay the entire
// topic) is opt-in via startFromBeginning and only applies when the consumer
// group has no committed offsets yet — an existing group always resumes from
// its last commit regardless of this setting.
func resolveStartOffset(startFromBeginning bool) int64 {
	if startFromBeginning {
		return kafka.FirstOffset
	}
	return kafka.LastOffset
}

// Close releases all Kafka resources. Call this once when the application shuts down.
func (a *KafkaAdapter) Close() {
	a.writer.Close()
	a.reader.Close()
	a.docWriter.Close()
	a.docReader.Close()
	if a.dlqWriter != nil {
		a.dlqWriter.Close()
	}
	if a.docDlqWriter != nil {
		a.docDlqWriter.Close()
	}
}

// EmitProcessingCompletedEvent publishes an image processing completed event.
// Uses rawPath as the message key to guarantee ordering per file.
func (a *KafkaAdapter) EmitProcessingCompletedEvent(ctx context.Context, rawPath, originalUrl, targetBucket string, results []port.ProcessingResult) error {
	eventResults := make([]ImageProcessingResult, len(results))
	for i, res := range results {
		meta := res.Metadata
		if meta == nil {
			meta = &processor.Metadata{}
		}
		eventResults[i] = ImageProcessingResult{
			TargetPath: res.TargetPath,
			Metadata: &MetadataEventField{
				Width:         meta.Width,
				Height:        meta.Height,
				SizeBytes:     meta.SizeBytes,
				MimeType:      meta.MimeType,
				FormatOrigin:  meta.FormatOrigin,
				BlurHash:      meta.BlurHash,
				DominantColor: meta.DominantColor,
				PHash:         meta.PHash,
				Entropy:       meta.Entropy,
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

// sendToDLQ republishes the original, untransformed message to the dead-letter
// writer, preserving the key and attaching diagnostic headers. It never mutates
// or drops the payload so the message stays byte-for-byte replayable. If the
// DLQ write itself fails, it is logged and swallowed — we must not block or
// crash the consumer loop over a DLQ outage.
func (a *KafkaAdapter) sendToDLQ(ctx context.Context, w kafkaWriter, m kafka.Message, cause error, stage string) {
	if w == nil {
		slog.Error("no DLQ writer configured, dropping message", "stage", stage, "offset", m.Offset, "error", cause)
		return
	}

	errMsg := ""
	if cause != nil {
		errMsg = cause.Error()
	}

	dlqMsg := kafka.Message{
		Key:   m.Key,
		Value: m.Value,
		Headers: []kafka.Header{
			{Key: "x-error", Value: []byte(errMsg)},
			{Key: "x-error-stage", Value: []byte(stage)},
			{Key: "x-original-topic", Value: []byte(m.Topic)},
			{Key: "x-original-partition", Value: []byte(strconv.Itoa(m.Partition))},
			{Key: "x-original-offset", Value: []byte(strconv.FormatInt(m.Offset, 10))},
			{Key: "x-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		},
	}

	if err := w.WriteMessages(ctx, dlqMsg); err != nil {
		slog.Error("failed to write message to DLQ", "error", err, "stage", stage, "offset", m.Offset)
	}
}

// handleWithRetry runs fn up to a.maxTries times, with exponential backoff and a
// per-attempt timeout (a.processTimeout), preserving the synchronous,
// commit-order-preserving semantics of the caller: it only returns once the
// message has either been committed (success or DLQ) or is intentionally left
// uncommitted for redelivery (shutdown mid-retry).
//
// A panic inside fn is recovered, logged with stack/topic/partition/offset,
// routed to the DLQ, and committed so the consumer loop survives to the next
// message.
func (a *KafkaAdapter) handleWithRetry(ctx context.Context, r kafkaReader, dlq kafkaWriter, m kafka.Message, stage string, fn func(context.Context) error) {
	defer func() {
		if rec := recover(); rec != nil {
			slog.Error("panic while handling message",
				"panic", rec,
				"stack", string(debug.Stack()),
				"topic", m.Topic,
				"partition", m.Partition,
				"offset", m.Offset,
			)
			a.sendToDLQ(ctx, dlq, m, fmt.Errorf("panic: %v", rec), "panic")
			if err := r.CommitMessages(ctx, m); err != nil {
				slog.Error("failed to commit after panic-DLQ", "error", err, "offset", m.Offset)
			}
		}
	}()

	var lastErr error
	backoff := a.retryBackoff
	for try := 1; try <= a.maxTries; try++ {
		msgCtx, cancel := context.WithTimeout(ctx, a.processTimeout)
		lastErr = fn(msgCtx)
		cancel()
		if lastErr == nil {
			if err := r.CommitMessages(ctx, m); err != nil {
				slog.Error("failed to commit message", "error", err, "offset", m.Offset)
			}
			return
		}
		if ctx.Err() != nil {
			// Shutting down: leave uncommitted, it will be redelivered.
			return
		}
		slog.Warn("handler failed, retrying", "error", lastErr, "try", try, "maxTries", a.maxTries, "offset", m.Offset, "stage", stage)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
		backoff *= 2
	}

	finalStage := stage
	if errors.Is(lastErr, context.DeadlineExceeded) {
		finalStage = "timeout"
	}
	a.sendToDLQ(ctx, dlq, m, lastErr, finalStage)
	if err := r.CommitMessages(ctx, m); err != nil {
		slog.Error("failed to commit after DLQ", "error", err, "offset", m.Offset)
	}
}

// Consume starts the image processing request consumer loop.
// It only commits the message offset after the handler returns successfully,
// after retries are exhausted (message is dead-lettered), or after a panic is
// recovered (message is dead-lettered). Otherwise the message is left
// uncommitted so Kafka redelivers it.
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
			// Malformed message: route to DLQ (preserved verbatim), then commit
			// so the partition doesn't stall on a message that can never be parsed.
			a.sendToDLQ(ctx, a.dlqWriter, msg, err, "unmarshal")
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

		// 1. Processamento Síncrono: A remoção da goroutine garante que os commits de Kafka
		// não ocorram fora de ordem por diferenças no tempo de processamento.
		func(m kafka.Message, e ImageProcessingRequestedEvent) {
			defer func() { <-a.imageSemaphore }()

			a.handleWithRetry(ctx, a.reader, a.dlqWriter, m, "handler", func(msgCtx context.Context) error {
				return handler(msgCtx, e.RawBucket, e.RawPath, e.OriginalUrl, e.TargetBucket, e.TargetPath, e.IsBackfill)
			})
		}(msg, event)
	}
}

// ConsumeDocumentRequests starts the document processing request consumer loop.
// It only commits the message offset after the handler returns successfully,
// after retries are exhausted (message is dead-lettered), or after a panic is
// recovered (message is dead-lettered). Otherwise the message is left
// uncommitted so Kafka redelivers it.
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
			a.healthy.Store(false)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		backoff = initialBackoff
		a.healthy.Store(true)

		var event DocumentProcessingRequestedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			slog.Error("failed to unmarshal document processing requested event", "error", err, "offset", msg.Offset)
			// Malformed message: route to DLQ (preserved verbatim), then commit
			// so the partition doesn't stall on a message that can never be parsed.
			a.sendToDLQ(ctx, a.docDlqWriter, msg, err, "unmarshal")
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

		// 1. Processamento Síncrono: A remoção da goroutine garante que os commits de Kafka
		// não ocorram fora de ordem por diferenças no tempo de processamento.
		func(m kafka.Message, e DocumentProcessingRequestedEvent) {
			defer func() { <-a.documentSemaphore }()

			a.handleWithRetry(ctx, a.docReader, a.docDlqWriter, m, "handler", func(msgCtx context.Context) error {
				return handler(msgCtx, e.RawBucket, e.RawPath, e.TargetBucket, e.TargetPath, e.Format)
			})
		}(msg, event)
	}
}

// IsReady returns true if the adapter is initialized and not in an error backoff state.
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
