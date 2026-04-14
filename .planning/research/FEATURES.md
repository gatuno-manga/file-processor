# Feature Landscape: Image Processing Microservice

**Domain:** High-performance Image Processing
**Researched:** 2025-02-14

## Table Stakes

Features users expect. Missing = product feels incomplete.

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| **Metadata Stripping** | Essential for privacy and size reduction. | Low | Use `bimg.Options{StripMetadata: true}`. |
| **Lossless WebP Conversion** | Standard for modern image formats. | Low | Use `bimg.Options{Type: bimg.WEBP, Lossless: true}`. |
| **In-Memory Buffering** | No disk I/O for speed. | Low | Use `[]byte` inputs/outputs. |
| **gRPC Synchronous API** | For real-time processing and metadata extraction. | Medium | Standard gRPC/Protobuf implementation. |
| **Kafka Asynchronous API** | For batch processing and event-driven architectures. | Medium | Standard Kafka consumer/producer implementation. |

## Differentiators

Features that set product apart. Not expected, but valued.

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| **Bounded Worker Pool** | Protects service from OOM and thrashing. | Medium | Sizes dynamically to `GOMAXPROCS`. |
| **Transactional Outbox** | Ensures consistency between state and Kafka events. | High | Requires database or reliable store for outbox events. |
| **Smart Caching (vips)** | Automatic tile caching for repeated operations. | Medium | Tunable via `vips_cache_set_max`. |
| **SIMD Acceleration** | Maximizes CPU throughput for resizing. | Low | Build-time requirement for libvips. |

## Anti-Features

Features to explicitly NOT build.

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| **Temporary Disk Storage** | Disk I/O is slow and hard to scale. | Use `[]byte` in memory. |
| **Native Go `image` package** | Extremely slow for heavy processing. | Stick with `libvips`. |
| **Multiple Binaries** | Complicates deployment. | Use structured concurrency to run gRPC/Kafka in one process. |

## Feature Dependencies

```
Core Image Logic → Bounded Worker Pool (Protects logic)
gRPC/Kafka Transport → Core Image Logic (Uses logic)
Transactional Outbox → Kafka Transport (Reliability layer)
```

## MVP Recommendation

Prioritize:
1. **Metadata Stripping & Lossless WebP Conversion** (Core logic).
2. **In-Memory processing** (Performance requirement).
3. **gRPC API** (Easiest way to test logic).
4. **Bounded Worker Pool** (Stability).

Defer:
- **Transactional Outbox:** Only needed if adding a database.
- **Advanced Format Support (AVIF):** Can be added in a later phase.

## Sources

- [libvips documentation on WebP](https://www.libvips.org/documentation/8.15/API/VipsImage.html#vips-image-write-to-buffer)
- [bimg Options Reference](https://pkg.go.dev/github.com/h2non/bimg#Options)
