# Phase 2: Synchronous API & Bounded Concurrency

## Goal
Expose the core image processing engine via gRPC and ensure stability under load using a bounded worker pool.

## User Decisions
- **D-01 (gRPC Contract):** Define service in `proto/v1/processor.proto`.
- **D-02 (Code Generation):** Generate code using `protoc` (standard Go plugins).
- **D-03 (Worker Pool):** Implement in `internal/pool/pool.go` using channels and goroutines.
- **D-04 (Concurrency):** Default pool size to `runtime.GOMAXPROCS(0)`.
- **D-05 (Server Location):** Implement gRPC server in `internal/api/grpc/server.go`.
- **D-06 (Context):** Ensure gRPC requests and processing respect `context.Context` timeouts.
- **D-07 (In-Memory):** Maintain in-memory operations as established in Phase 1.
- **D-08 (Testing):** Add unit tests for both the worker pool and the gRPC server.

## Success Criteria (what must be TRUE)
- A gRPC server is active and can process image requests via Protobuf-defined messages.
- All image processing tasks are routed through a bounded worker pool, preventing system OOM.
- The worker pool size is configurable via environment variables, defaulting to `GOMAXPROCS`.
- gRPC requests respect context timeouts, ensuring long-running processes are cancelled correctly.
- In-memory processing is maintained (no disk I/O).

## Blocker Decisions
- None.

## the agent's Discretion
- Choice of proto package name (e.g., `gatuno.v1`).
- Choice of specific Protobuf field types (likely `bytes input_image` and `bytes output_image`).
- Error handling patterns for gRPC (use `google.golang.org/grpc/status`).
