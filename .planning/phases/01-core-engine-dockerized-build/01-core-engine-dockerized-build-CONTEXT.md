# Phase 1: Core Engine & Dockerized Build - CONTEXT

## Phase Goal
Establish a stable build environment and implement the core image transformation logic (metadata stripping, lossless WebP conversion) using Go and `libvips` (via `bimg`) on Alpine Linux.

## Decisions

### D-01: Project Structure
- Use the following folder structure:
  - `cmd/`: Application entrypoints.
  - `internal/processor`: Core image processing logic.
  - `pkg/`: Shared utility packages.

### D-02: Runtime & Build Environment
- Use **Go 1.23+** and **Alpine Linux 3.20** as the base image for Docker.
- Implement a **multi-stage Docker build** to ensure minimal runtime image size and proper management of `libvips` dependencies (using `vips-dev` for build, `vips` for runtime).

### D-03: Image Engine (`bimg`)
- Use the `github.com/h2non/bimg` library as the `libvips` wrapper.
- Configure `libvips` **cache limits** explicitly at startup (e.g., `bimg.VipsCacheSetMax(0)` and `bimg.VipsCacheSetMaxMem(0)` for minimal memory usage if needed, or sensible defaults).

### D-04: Image Processing Logic
- The core processing function must take an **in-memory byte buffer** (`[]byte`) as input and return an **in-memory byte buffer** as output (no disk I/O).
- Apply **auto-rotation** before stripping metadata to preserve visual orientation.
- **Strip all metadata** (EXIF, IPTC, XMP).
- Convert images to **lossless WebP** format.

### D-05: Unit Testing
- Include unit tests for the core processing function in `internal/processor` that verify metadata stripping and WebP conversion.

## Deferred Ideas
- gRPC API implementation (Phase 2).
- Kafka integration (Phase 3).
- Graceful shutdown logic (Phase 4).
