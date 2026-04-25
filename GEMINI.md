# Gatuno - File Processor & Storage Optimizer

High-performance worker focused on storage efficiency and privacy through file compression and data sanitization.

## 🚀 Core Value

- **Main Objective:** Reduce the storage footprint of **any file** through optimized compression.
- **Image Strategy:** Mandatory conversion to **WebP** (Lossless/Lossy). Chosen for its performance, native support on modern devices, and drastic size reduction without perceptible visual loss.
- **Privacy:** Removal of sensitive metadata (EXIF/IPTC) during processing.
- **Performance:** Go engine with strictly in-memory processing via `libvips`.

## 🛠️ Tech Stack

- **Language:** Go 1.23+
- **Image Processing:** `libvips` (via `bimg`)
- **Transport:** gRPC (Synchronous) and Kafka (Asynchronous for batch processing)
- **Storage Integration:** S3-compatible adapter for persisting optimized files

## 🏗️ Engineering Principles

### Architecture & Design (Clean Architecture)
- **Hexagonal Architecture (Ports and Adapters):** Keep business logic (`processor`, `pool`) strictly isolated from external delivery mechanisms (`grpc`, `kafka`, `s3`).
- **Memory-First:** Processing via byte buffers in RAM; avoid temporary disk I/O in the worker.
- **Bounded Concurrency:** Worker pool limited to `GOMAXPROCS` to protect the system against load spikes (OOM prevention).
- **WebP First:** For images, WebP is the output standard due to the optimal balance between compression and client decoding time.

### Clean Code
- **Meaningful Names:** Use descriptive variables and functions. Avoid acronyms unless universally understood.
- **Small Functions:** Keep functions short and focused on a single responsibility.
- **Error Handling:** Never fail silently. Errors must be logged with context or returned up the call stack appropriately. Avoid `panic` in production code.

### Quality & Testing
- **Test Coverage:** All business logic, adapters, and orchestrators must have unit tests.
- **Integration Tests:** Use `load_test.go` and `stability_test.go` to ensure memory safety under high concurrency (1 to 1000 simultaneous files).
- **External Dependencies:** When using `bimg`, always handle CGO memory constraints properly to avoid memory leaks.

## 📁 Directory Structure

- `/cmd/server`: gRPC and Kafka entry points.
- `/internal/processor`: Compression and sanitization engines (Domain).
- `/internal/pool`: Resource orchestration and concurrency limits (Domain).
- `/internal/adapter`: S3 (Storage) and Kafka (Events) implementations (Infrastructure).
- `/proto/v1`: API contracts for cross-service integration.

## 🚦 Operations

- **Build:** `docker compose build`
- **Load Testing:** `./benchmark.sh` (Verifies stability across 1, 10, 100, 1000 simultaneous files).
- **Quality Check:** `./quality-test.sh` (Verifies sanitization integrity).
