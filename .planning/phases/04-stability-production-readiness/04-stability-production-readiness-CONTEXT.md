# Context: Phase 04 - Stability & Production Readiness

## Goal
Finalize memory cache limits and telemetry for production readiness.

## User Decisions (from prompt)
1. Implement health checks (Liveness and Readiness probes) for both gRPC and Kafka. (D-01)
2. Refine `libvips` cache limits (`bimg.VipsCacheSetMax` and `bimg.VipsCacheSetMaxMem`) for stability. (D-02)
3. Improve logging (structured logging with `slog` or similar). (D-03)
4. Add metrics (e.g., Prometheus) for image processing throughput and latency. (D-04)
5. Ensure graceful shutdown for gRPC, Kafka, and the worker pool. (D-05)
6. Finalize `internal/processor/config.go`. (D-06)
7. Update `cmd/server/main.go` to include health checks and improved logging. (D-07)
8. Add unit tests for health checks and shutdown logic. (D-08)
9. Verify performance and stability under load (if possible, with mocks). (D-09)
10. Ensure all operations are in-memory. (D-10)
11. Finalize the `Dockerfile` for production. (D-11)
12. Ensure all operations are in-memory (redundant). (D-12)

## Constraints
- **In-memory**: All image processing MUST occur in-memory. (C-01)
- **libvips**: Use `bimg` to manage cache limits. (C-02)
- **CGO/musl**: Must be handled correctly in Alpine-based Dockerfile. (C-03)

## Research Findings
- `bimg.VipsCacheSetMax(0)` disables the operation cache, which is safest for memory-constrained environments but may impact performance if same operations are repeated. (R-01)
- gRPC `GracefulStop` waits for all RPCs to finish. (R-02)
- `kafka-go` Reader needs careful closure and context handling to stop fetching. (R-03)
- `slog` is the standard library structured logger since Go 1.21. (R-04)
