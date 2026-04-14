# Technology Stack: Image Processor Microservice

**Project:** Image Processor
**Researched:** 2025-02-14

## Recommended Stack

### Core Framework & Language
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| Go | 1.23+ | Language | Native concurrency, fast startup, small binaries. |
| gRPC | 1.6x | Sync API | Type-safe communication, efficient Protobuf serialization. |
| Kafka | 3.x | Async Messaging | Event-driven processing for heavy image tasks. |

### Image Processing
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| [bimg](github.com/h2non/bimg) | 1.1.9+ | Image Lib | CGO wrapper for `libvips`; extremely fast, low memory. |
| [libvips](https://www.libvips.org/) | 8.15+ | Engine | Demand-driven architecture, tiles-based processing. |

### Infrastructure
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| Docker | Latest | Packaging | Multi-stage build handles complex C dependencies. |
| Alpine Linux | 3.20 | Base Image | Minimal footprint, recent `libvips` in package repo. |

### Supporting Libraries
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| [segmentio/kafka-go](github.com/segmentio/kafka-go) | Latest | Kafka Client | Easier to build on Alpine (no CGO required if using pure Go parts). |
| [twmb/franz-go](github.com/twmb/franz-go) | Latest | Kafka Client | Use for ultra-high performance and transaction support. |
| [conc](github.com/sourcegraph/conc) | Latest | Concurrency | Modern structured concurrency wrapper for errgroup/workerpool. |

## Alternatives Considered

| Category | Recommended | Alternative | Why Not |
|----------|-------------|-------------|---------|
| Image Lib | `bimg` (libvips) | Native `image` | Native is significantly slower and uses more memory for large images. |
| Kafka Lib | `segmentio/kafka-go` | `confluent-kafka-go` | Confluent depends on `librdkafka` (C), which is harder to manage in Alpine. |
| Distro | Alpine | Debian Slim | Debian is easier for CGO, but Alpine's images are 5-10x smaller. |

## Installation

```bash
# Core Dependencies
go get github.com/h2non/bimg
go get github.com/segmentio/kafka-go
go get google.golang.org/grpc
go get golang.org/x/sync/errgroup

# Local (Ubuntu/Debian) for development
sudo apt-get install libvips-dev

# Local (macOS)
brew install vips
```

## Sources

- [bimg GitHub](https://github.com/h2non/bimg)
- [libvips Official Docs](https://www.libvips.org/documentation/)
- [Standard Go Project Layout](https://github.com/golang-standards/project-layout)
