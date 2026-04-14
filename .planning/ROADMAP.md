# Roadmap: Gatuno (Image Sanitizer and Optimizer Worker)

## Project Overview
Gatuno is a high-performance image processing microservice designed to sanitize (strip metadata) and optimize (lossless WebP) images. It uses Go, `libvips` (via `bimg`), and supports both gRPC and Kafka.

## Phases
- [ ] **Phase 1: Core Engine & Dockerized Build** - Build the image transformation logic and Alpine-based build pipeline.
- [ ] **Phase 2: Synchronous API & Bounded Concurrency** - Implement gRPC service and worker pool for stable, real-time processing.
- [ ] **Phase 3: Asynchronous Kafka Integration** - Add event-driven processing capabilities via Kafka.
- [ ] **Phase 4: Stability & Production Readiness** - Implement graceful shutdown and final resilience tuning.

---

## Phase Details

### Phase 1: Core Engine & Dockerized Build
**Goal**: Establish a stable build environment and implement the core image transformation logic.
**Depends on**: Nothing
**Requirements**: CORE-01, CORE-02, CORE-03, CORE-04, ENV-01, ENV-02, REL-03
**Success Criteria** (what must be TRUE):
  1. A multi-stage Dockerfile successfully builds the Go application on Alpine Linux with all `libvips` dependencies.
  2. The core processing function can take an image buffer, strip all EXIF/IPTC/XMP metadata, and return a lossless WebP buffer entirely in-memory.
  3. Images with orientation metadata are correctly rotated before stripping to preserve visual orientation.
  4. `libvips` cache limits are explicitly configured at startup to prevent unbounded memory growth.
**Plans**:
- [x] 01-01-PLAN.md — Setup Dockerized Alpine build environment and project skeleton.
- [x] 01-02-PLAN.md — Implement core image transformation engine and unit tests.

### Phase 2: Synchronous API & Bounded Concurrency
**Goal**: Expose the core engine via gRPC and ensure stability under load using a bounded worker pool.
**Depends on**: Phase 1
**Requirements**: POOL-01, POOL-02, SYNC-01, SYNC-02, REL-01
**Success Criteria** (what must be TRUE):
  1. A gRPC server is active and can process image requests via Protobuf-defined messages.
  2. All image processing tasks are routed through a bounded worker pool, preventing system OOM.
  3. The worker pool size is configurable via environment variables, defaulting to `GOMAXPROCS`.
  4. gRPC requests respect context timeouts, ensuring long-running processes are cancelled correctly.
**Plans**: TBD

### Phase 3: Asynchronous Kafka Integration
**Goal**: Integrate Gatuno into event-driven workflows using Kafka topics.
**Depends on**: Phase 2
**Requirements**: ASYNC-01, ASYNC-02
**Success Criteria** (what must be TRUE):
  1. The service consumes image processing jobs from a configured Kafka input topic.
  2. Processed results (image bytes) are published to a configured Kafka output topic.
  3. Kafka consumer uses the same bounded worker pool for processing to maintain system stability.
**Plans**: TBD

### Phase 4: Stability & Production Readiness
**Goal**: Finalize resilience features and ensure the service is ready for high-availability environments.
**Depends on**: Phase 3
**Requirements**: REL-02
**Success Criteria** (what must be TRUE):
  1. The service implements graceful shutdown, allowing gRPC and Kafka listeners to finish in-flight work before exiting.
  2. The system remains stable under peak load without memory leaks (verified via basic load testing/profiling).
**Plans**: TBD

---

## Progress Table

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Core Engine & Dockerized Build | 0/2 | In progress | - |
| 2. Synchronous API & Bounded Concurrency | 0/0 | Not started | - |
| 3. Asynchronous Kafka Integration | 0/0 | Not started | - |
| 4. Stability & Production Readiness | 0/0 | Not started | - |
