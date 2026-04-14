# Discovery: Kafka and Storage Libraries (Phase 3)

## Goal
Research Go Kafka and Object Storage libraries for Gatuno to support asynchronous image processing.

## Kafka Libraries [D-01]

### Option A: `segmentio/kafka-go`
- **Type**: Pure Go (no CGO required by default).
- **Pros**:
  - Easier to build and maintain in Alpine Linux.
  - Better integration with Go standard library (context support).
  - Modern API.
  - No need for `librdkafka`.
- **Cons**:
  - Potentially lower throughput than `librdkafka` for extreme use cases.
- **Verdict**: Recommended for Gatuno due to simplicity and ease of build in Alpine/CGO environment where `libvips` is already present.

### Option B: `confluent-kafka-go`
- **Type**: CGO (wraps `librdkafka`).
- **Pros**:
  - Industry standard for performance and compatibility.
  - Full feature set of Kafka protocol.
- **Cons**:
  - Requires `librdkafka` to be installed in the build and runtime environments.
  - Adds build complexity to Alpine Docker image.
- **Verdict**: Not recommended unless `segmentio/kafka-go` lacks a critical feature.

## Object Storage Libraries [D-06]

### Option A: `minio-go`
- **Type**: Official SDK for MinIO.
- **Pros**:
  - Best S3 compatibility.
  - Excellent for in-memory operations (using `io.Reader`/`io.Writer`).
  - Active community.
- **Verdict**: Recommended for Gatuno.

### Option B: `aws-sdk-go-v2`
- **Type**: Official AWS SDK.
- **Pros**:
  - More robust for AWS-specific features (IAM, etc.).
- **Cons**:
  - Larger footprint.
- **Verdict**: Overkill for simple S3/MinIO downloads/uploads.

## Integration Pattern
- Use **Hexagonal Architecture** for Kafka and Storage adapters.
- Define internal ports (interfaces) in `internal/port/kafka.go` and `internal/port/storage.go`.
- Implement adapters in `internal/adapter/kafka` and `internal/adapter/storage`.
- Orchestration logic in `internal/orchestrator` to handle the async flow:
  `Consumer` -> `Event` -> `Downloader` -> `Pool` -> `Processor` -> `Uploader` -> `Producer`.
- Maintain all operations in RAM (no temporary files).
