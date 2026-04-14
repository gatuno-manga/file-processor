# Requirements: Gatuno (Image Sanitizer and Optimizer Worker)

## v1 Requirements

### CORE: Image Processing Logic
| ID | Requirement | Category |
|----|-------------|----------|
| CORE-01 | Strip all metadata (EXIF, IPTC, XMP) from input images. | CORE |
| CORE-02 | Convert input images to lossless WebP format. | CORE |
| CORE-03 | Apply auto-rotation before stripping metadata to maintain orientation. | CORE |
| CORE-04 | Ensure all image processing occurs entirely in-memory using byte buffers. | CORE |

### POOL: Concurrency Management
| ID | Requirement | Category |
|----|-------------|----------|
| POOL-01 | Implement a bounded worker pool for image processing tasks. | POOL |
| POOL-02 | Allow pool size configuration, defaulting to `GOMAXPROCS`. | POOL |

### SYNC: Synchronous gRPC API
| ID | Requirement | Category |
|----|-------------|----------|
| SYNC-01 | Define gRPC Protobuf for image processing service. | SYNC |
| SYNC-02 | Implement gRPC server handling requests with image byte buffers. | SYNC |

### ASYNC: Asynchronous Kafka Integration
| ID | Requirement | Category |
|----|-------------|----------|
| ASYNC-01 | Implement Kafka consumer to receive image processing jobs. | ASYNC |
| ASYNC-02 | Implement Kafka producer to publish results of processed images. | ASYNC |

### ENV: Runtime & Build
| ID | Requirement | Category |
|----|-------------|----------|
| ENV-01 | Create multi-stage Docker build for libvips on Alpine Linux. | ENV |
| ENV-02 | Ensure correct management of CGO/musl for bimg/libvips in Docker. | ENV |

### REL: Reliability & Resilience
| ID | Requirement | Category |
|----|-------------|----------|
| REL-01 | Apply context-aware timeouts across all processing stages. | REL |
| REL-02 | Implement graceful shutdown for gRPC and Kafka listeners. | REL |
| REL-03 | Configure libvips cache limits to prevent memory growth/leaks. | REL |

## v2 Requirements (Deferred)
- **REL-04:** Transactional Outbox pattern for Kafka reliability.
- **CORE-05:** Support for additional output formats (e.g., AVIF).
- **MON-01:** Advanced Prometheus metrics for processing latency and pool utilization.

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| CORE-01 | Phase 1 | Complete |
| CORE-02 | Phase 1 | Complete |
| CORE-03 | Phase 1 | Complete |
| CORE-04 | Phase 1 | Complete |
| POOL-01 | Phase 2 | Pending |
| POOL-02 | Phase 2 | Pending |
| SYNC-01 | Phase 2 | Pending |
| SYNC-02 | Phase 2 | Pending |
| ASYNC-01 | Phase 3 | Pending |
| ASYNC-02 | Phase 3 | Pending |
| ENV-01 | Phase 1 | Complete |
| ENV-02 | Phase 1 | Complete |
| REL-01 | Phase 2 | Pending |
| REL-02 | Phase 4 | Pending |
| REL-03 | Phase 1 | Complete |
