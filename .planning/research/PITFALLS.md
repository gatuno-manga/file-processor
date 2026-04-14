# Domain Pitfalls: Image Processing Microservice

**Domain:** High-performance Image Processing
**Researched:** 2025-02-14

## Critical Pitfalls

### Pitfall 1: CGO Overhead vs. Runtime Performance
**What goes wrong:** Developers worry about the cost of CGO calls and attempt to batch operations in inefficient ways.
**Why it happens:** Misunderstanding that image processing is a "heavy" task where 40ns overhead is negligible compared to a 10ms processing time.
**Consequences:** Over-engineering the bridge between Go and C.
**Prevention:** Focus on reducing the number of CGO calls in tight loops (e.g., pixel-by-pixel), but use high-level `bimg` methods (`Resize`, `Process`) freely.

### Pitfall 2: `libvips` Cache Memory Leak
**What goes wrong:** The `libvips` cache grows indefinitely, causing Out-of-Memory (OOM) errors.
**Why it happens:** By default, `libvips` keeps a large cache of tiles and operations to speed up repeated tasks.
**Consequences:** Microservice crashes under sustained high load.
**Prevention:** Tune the cache size during initialization:
```go
// in main.go or init()
bimg.VipsCacheSetMax(100)      // Max items in cache
bimg.VipsCacheSetMaxMem(100)   // Max memory (in MB) for cache
```

### Pitfall 3: Musl vs Glibc in Alpine Linux
**What goes wrong:** The Go binary compiled in a Glibc-based environment (e.g., standard Debian/Ubuntu) fails to run on Alpine.
**Why it happens:** Alpine uses the `musl` C library, which is incompatible with `glibc`.
**Consequences:** "File not found" errors when executing the binary.
**Prevention:** Always use a multi-stage Docker build with `golang:alpine` as the builder and `alpine` as the runner. Ensure `CGO_ENABLED=1` is set.

## Moderate Pitfalls

### Pitfall 1: Bounded Worker Pool Mis-sizing
**What goes wrong:** Worker pool is sized to `runtime.NumCPU()`, which may reflect the host machine's cores rather than the container's CPU quota.
**Prevention:** Use `runtime.GOMAXPROCS(0)` instead.

### Pitfall 2: Kafka Consumer Lag on Heavy Images
**What goes wrong:** A few very large images block a Kafka partition for other small images.
**Prevention:** Implement a separate "priority queue" or different topics for "small" vs "large" image processing if processing times vary significantly.

## Minor Pitfalls

### Pitfall 1: Metadata Stripping vs. Orientation
**What goes wrong:** Stripping metadata sometimes removes the `Orientation` tag, causing images to appear sideways.
**Prevention:** Ensure `bimg.Options{AutoRotate: true}` is used before stripping metadata to bake the rotation into the pixel data.

## Phase-Specific Warnings

| Phase Topic | Likely Pitfall | Mitigation |
|-------------|---------------|------------|
| **Foundation** | libvips install issues | Use the provided multi-stage Dockerfile from day one. |
| **Worker Pool** | Goroutine leaks | Always use `sync.WaitGroup` and `context` for graceful shutdown. |
| **Kafka Integration** | At-least-once delivery duplicates | Ensure image processing is idempotent (e.g., by using unique file hashes as keys). |

## Sources

- [libvips memory management](https://www.libvips.org/documentation/8.15/API/VipsOperation.html#vips-operation-get-type)
- [bimg Issue Tracker: Memory usage](https://github.com/h2non/bimg/issues)
- [Go CGO performance benchmarks](https://dave.cheney.net/2016/01/18/cgo-is-not-go)
