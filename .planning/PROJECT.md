# Project: Gatuno (Image Sanitizer and Optimizer Worker)

## Core Value
Gatuno provides high-performance image sanitization and optimization for private digital libraries. It ensures user privacy by stripping metadata and improves storage efficiency by converting images to lossless WebP format, all while maintaining high throughput and low memory footprint using Go and libvips.

## Target Audience
- Private digital library owners
- Privacy-conscious developers
- High-traffic image hosting services

## Constraints
- **Language:** Go 1.21+ (preferably 1.23+ as per research)
- **Image Engine:** `bimg` (wrapping `libvips`)
- **Transport:** gRPC (sync) and Kafka (async)
- **Runtime:** Alpine Linux (requires careful CGO/musl management)
- **Memory:** Strict in-memory operations (no disk I/O for processing)
- **Concurrency:** Bounded worker pool to prevent OOM

## Success Definition
- 100% of images processed have all metadata (EXIF, IPTC, etc.) removed.
- 100% of output images are in lossless WebP format.
- Processing is performed entirely in RAM.
- Service remains stable under heavy load without exceeding memory limits.
