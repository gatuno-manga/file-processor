# Requirements: Gatuno (Image Sanitizer and Optimizer Worker)

## v1 Requirements

### CORE: Image Processing Logic
| ID | Requirement | Category | Status |
|----|-------------|----------|--------|
| CORE-01 | Strip all metadata (EXIF, IPTC, XMP) from input images. | CORE | Complete |
| CORE-02 | Convert input images to lossless WebP format. | CORE | Complete |
| CORE-03 | Apply auto-rotation before stripping metadata to maintain orientation. | CORE | Complete |
| CORE-04 | Ensure all image processing occurs entirely in-memory using byte buffers. | CORE | Complete |

### POOL: Concurrency Management
| ID | Requirement | Category | Status |
|----|-------------|----------|--------|
| POOL-01 | Implement a bounded worker pool for image processing tasks. | POOL | Complete |
| POOL-02 | Allow pool size configuration, defaulting to `GOMAXPROCS`. | POOL | Complete |

### SYNC: Synchronous gRPC API
| ID | Requirement | Category | Status |
|----|-------------|----------|--------|
| SYNC-01 | Define gRPC Protobuf for image processing service. | SYNC | Complete |
| SYNC-02 | Implement gRPC server handling requests with image byte buffers. | SYNC | Complete |

### ASYNC: Asynchronous Kafka Integration
| ID | Requirement | Category | Status |
|----|-------------|----------|--------|
| ASYNC-01 | Implement Kafka consumer to receive image processing jobs. | ASYNC | Complete |
| ASYNC-02 | Implement Kafka producer to publish results of processed images. | ASYNC | Complete |

### ENV: Runtime & Build
| ID | Requirement | Category | Status |
|----|-------------|----------|--------|
| ENV-01 | Create multi-stage Docker build for libvips on Alpine Linux. | ENV | Complete |
| ENV-02 | Ensure correct management of CGO/musl for bimg/libvips in Docker. | ENV | Complete |
| ENV-03 | Centralize configuration management with environment variable overrides. | ENV | Complete |
| ENV-04 | Finalize security-hardened production Dockerfile with non-root user. | ENV | Complete |

### REL: Reliability & Resilience
| ID | Requirement | Category | Status |
|----|-------------|----------|--------|
| REL-01 | Apply context-aware timeouts across all processing stages. | REL | Complete |
| REL-02 | Implement graceful shutdown for gRPC and Kafka listeners. | REL | Complete |
| REL-03 | Configure libvips cache limits to prevent memory growth/leaks. | REL | Complete |
| REL-04 | Add automated verification for health check and shutdown logic. | REL | Complete |
| REL-05 | Verify stability and in-memory processing under high load. | REL | Complete |

### MON: Monitoring & Observability
| ID | Requirement | Category | Status |
|----|-------------|----------|--------|
| MON-01 | Implement Prometheus metrics for processing latency and throughput. | MON | Complete |
| MON-02 | Implement Liveness and Readiness HTTP health probes. | MON | Complete |
| MON-03 | Implement structured logging using `slog` for production observability. | MON | Complete |

## v2 Requirements (Deferred)
- **REL-06:** Transactional Outbox pattern for Kafka reliability.
- **CORE-05:** Support for additional output formats (e.g., AVIF).

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| CORE-01 | Phase 1 | Complete |
| CORE-02 | Phase 1 | Complete |
| CORE-03 | Phase 1 | Complete |
| CORE-04 | Phase 4 | Complete |
| POOL-01 | Phase 2 | Complete |
| POOL-02 | Phase 2 | Complete |
| SYNC-01 | Phase 2 | Complete |
| SYNC-02 | Phase 2 | Complete |
| ASYNC-01 | Phase 3 | Complete |
| ASYNC-02 | Phase 3 | Complete |
| ENV-01 | Phase 1 | Complete |
| ENV-02 | Phase 1 | Complete |
| ENV-03 | Phase 4 | Complete |
| ENV-04 | Phase 4 | Complete |
| REL-01 | Phase 2 | Complete |
| REL-02 | Phase 4 | Complete |
| REL-03 | Phase 4 | Complete |
| REL-04 | Phase 4 | Complete |
| REL-05 | Phase 4 | Complete |
| MON-01 | Phase 4 | Complete |
| MON-02 | Phase 4 | Complete |
| MON-03 | Phase 4 | Complete |
