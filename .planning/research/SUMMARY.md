# Research Summary: Image Processor Microservice

## Executive Summary

The Image Processor is a high-performance microservice designed to handle heavy image transformation tasks—specifically metadata stripping and lossless WebP conversion—with minimal latency and memory overhead. The service utilizes **Go** for its superior concurrency primitives and **libvips** (via the `bimg` wrapper) for its demand-driven, tiled image processing engine, which significantly outperforms standard Go image libraries.

The recommended approach uses a **Hexagonal Architecture** to decouple the core image logic from its transport layers, supporting both synchronous **gRPC** calls for real-time processing and asynchronous **Kafka** events for batch workloads. To ensure stability under load, the service must implement a **bounded worker pool** and strict **memory cache limits** for `libvips`. The primary deployment target is Alpine Linux via multi-stage Docker builds to keep the footprint small while managing the necessary C dependencies.

## Key Findings

### 🛠 Technology Stack (from STACK.md)
- **Core:** Go 1.23+ for the application logic, gRPC for sync APIs, and Kafka 3.x for async messaging.
- **Image Engine:** `libvips` 8.15+ (via `bimg` 1.1.9) is selected for its low memory footprint and high speed.
- **Runtime:** Alpine Linux 3.20 is preferred for its small size, despite the requirement for careful CGO/musl management.
- **Concurrency:** `segmentio/kafka-go` and `sourcegraph/conc` are recommended for modern, structured concurrency and Kafka integration.

### ✨ Features & Requirements (from FEATURES.md)
- **Table Stakes:** Metadata stripping, lossless WebP conversion, in-memory processing (no slow disk I/O), and dual API support (gRPC/Kafka).
- **Differentiators:** Bounded worker pools sized to `GOMAXPROCS` to prevent OOM, and Transactional Outbox for Kafka reliability.
- **Anti-Features:** Avoid native Go `image` package (too slow) and temporary disk storage (bottleneck).

### 🏗 Architecture Patterns (from ARCHITECTURE.md)
- **Pattern:** Hexagonal (Ports & Adapters) to isolate `internal/domain` and `internal/app` from infra details.
- **Concurrency Pattern:** Use `errgroup` for structured lifecycle management of gRPC and Kafka servers in a single process.
- **Data Flow:** All processing happens in memory using `[]byte` buffers, passed through a fixed-size worker pool to manage CPU/Memory saturation.

### ⚠️ Critical Pitfalls (from PITFALLS.md)
- **Memory Leaks:** `libvips` cache can grow indefinitely; must be explicitly capped using `VipsCacheSetMax`.
- **CGO/Alpine Conflict:** Go binaries built on Glibc won't run on Alpine; requires multi-stage Docker builds using `golang:alpine`.
- **Metadata Orientation:** Stripping EXIF can rotate images incorrectly; `AutoRotate: true` must be applied first.

## Implications for Roadmap

### Suggested Phase Structure

1.  **Phase 1: Core Foundation & Dockerization**
    *   **Rationale:** Establish the build pipeline early to solve CGO/Alpine compatibility.
    *   **Deliverables:** Multi-stage Dockerfile, `libvips` integration, and core transformation logic (WebP/Strip).
    *   **Pitfalls to Avoid:** Glibc/musl mismatches.

2.  **Phase 2: Sync API & Worker Pool**
    *   **Rationale:** Provides a testable interface (gRPC) and implements the stability layer (Worker Pool).
    *   **Deliverables:** gRPC service, Protobuf definitions, and bounded worker pool implementation.
    *   **Pitfalls to Avoid:** OOM from uncapped workers.

3.  **Phase 3: Async Messaging (Kafka)**
    *   **Rationale:** Adds event-driven capabilities once the core and pool are stable.
    *   **Deliverables:** Kafka consumer/producer adapters, structured concurrency (errgroup).
    *   **Pitfalls to Avoid:** Consumer lag on heavy images; lack of idempotency.

4.  **Phase 4: Reliability & Tuning**
    *   **Rationale:** Hardens the service for production load.
    *   **Deliverables:** `libvips` cache tuning, graceful shutdown logic, and monitoring/telemetry.
    *   **Pitfalls to Avoid:** Memory leaks in long-running processes.

### Research Flags
- **Needs Research:** Detailed Kafka partitioning strategy for large vs. small images (if load is heterogeneous).
- **Standard Patterns:** Hexagonal architecture and gRPC/Kafka implementations follow well-documented industry standards.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| **Stack** | HIGH | `libvips` + Go is the industry standard for high-performance image services. |
| **Features** | HIGH | Requirements are clear and align with common image processing needs. |
| **Architecture** | HIGH | Hexagonal architecture is well-suited for multiple transport layers. |
| **Pitfalls** | MEDIUM | `libvips` memory management is tricky and may require tuning under real-world load. |

### Gaps to Address
- No specific strategy yet for "priority" processing of small images if the Kafka queue gets backed up by large ones.
- Transactional Outbox implementation details are deferred (High complexity).

## Sources
- [bimg GitHub & Documentation](https://github.com/h2non/bimg)
- [libvips Official Documentation](https://www.libvips.org/)
- [Standard Go Project Layout](https://github.com/golang-standards/project-layout)
- [Dave Cheney: CGO Performance](https://dave.cheney.net/2016/01/18/cgo-is-not-go)
