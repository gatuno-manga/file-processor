# Phase 3: Asynchronous Kafka Integration - Context

## Objective
Add event-driven Kafka support for asynchronous image processing.

## Requirements (from user instructions)
- [D-01] Research Go Kafka libraries: `segmentio/kafka-go` (pure Go) vs. `confluent-kafka-go` (CGO).
- [D-02] Define Kafka consumer for processing image download events (e.g., `image.downloaded`).
- [D-03] Define Kafka producer for emitting `file.sanitized` events.
- [D-04] Implement Kafka adapter in `internal/adapter/kafka/adapter.go`.
- [D-05] Integrate the Kafka adapter with the existing worker pool.
- [D-06] Support Object Storage (S3/MinIO) downloads and uploads (using `minio-go` or similar).
- [D-07] Ensure `context.Context` and timeouts are respected for Kafka processing.
- [D-08] Update `cmd/server/main.go` to start the Kafka consumer alongside the gRPC server.
- [D-09] Add unit tests for Kafka integration (using mocks).
- [D-10] Ensure in-memory byte buffer operations are maintained.
- [D-11] Verify the flow: Kafka Event -> Download -> Pool -> Process -> Upload -> Kafka Event.
- [D-12] Ensure all operations are in-memory.

## Constraints
- Language: Go 1.23+
- Image Engine: `bimg` (wrapping `libvips`)
- Transport: Kafka (async)
- Storage: S3-compatible (MinIO/S3)
- Memory: Strict in-memory operations (no disk I/O for processing)
- Concurrency: Bounded worker pool to prevent OOM
