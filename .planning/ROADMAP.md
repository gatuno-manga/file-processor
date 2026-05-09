# Roadmap: Gatuno (Image Sanitizer and Optimizer Worker)

## Project Overview
Gatuno is a high-performance image processing microservice designed to sanitize (strip metadata) and optimize (lossless WebP) images. It uses Go, `libvips` (via `bimg`), and supports both gRPC and Kafka.

## Phases
- [x] **Phase 1: Core Engine & Dockerized Build** - Build the image transformation logic and Alpine-based build pipeline.
- [x] **Phase 2: Synchronous API & Bounded Concurrency** - Implement gRPC service and worker pool for stable, real-time processing.
- [x] **Phase 3: Asynchronous Kafka Integration** - Add event-driven processing capabilities via Kafka.
- [x] **Phase 4: Stability & Production Readiness** - Implement graceful shutdown and final resilience tuning.

---

## Phase Details

### Phase 1: Core Engine & Dockerized Build
**Goal**: Establish a stable build environment and implement the core image transformation logic.
**Status**: Complete (2024-04-14)

### Phase 2: Synchronous API & Bounded Concurrency
**Goal**: Expose the core engine via gRPC and ensure stability under load using a bounded worker pool.
**Status**: Complete (2024-10-31)

### Phase 3: Asynchronous Kafka Integration
**Goal**: Integrate Gatuno into event-driven workflows using Kafka topics.
**Status**: Complete (2024-10-31)

### Phase 4: Stability & Production Readiness
**Goal**: Finalize memory cache limits and telemetry for production readiness.
**Status**: Complete (2024-10-31)

---

## Progress Table

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Core Engine & Dockerized Build | 2/2 | Complete | 2024-04-14 |
| 2. Synchronous API & Bounded Concurrency | 3/3 | Complete | 2024-10-31 |
| 3. Asynchronous Kafka Integration | 3/3 | Complete | 2024-10-31 |
| 4. Stability & Production Readiness | 3/3 | Complete | 2024-10-31 |
