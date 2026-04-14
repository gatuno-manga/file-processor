# Architecture Patterns: Image Processing Microservice

**Domain:** High-performance Image Processing
**Researched:** 2025-02-14

## Recommended Architecture

The service follows a **Hexagonal Architecture** (Ports and Adapters) pattern, allowing it to support multiple transports (gRPC, Kafka) while keeping the core image processing logic decoupled.

### Component Boundaries

| Component | Responsibility | Communicates With |
|-----------|---------------|-------------------|
| **Entrypoint (`cmd/`)** | App initialization, dependency injection, and lifecycle management. | All internal layers. |
| **Domain (`internal/domain`)** | Core models (Image, ProcessingResult) and interfaces. | All layers. |
| **Use Case (`internal/app`)** | Orchestrates image processing using the domain interfaces. | Infrastructure adapters. |
| **gRPC Adapter (`internal/infra/grpc`)** | Handles incoming sync requests, converts to domain models. | Use cases. |
| **Kafka Adapter (`internal/infra/kafka`)** | Consumes events and produces results via Kafka topics. | Use cases. |
| **Image Engine (`internal/infra/bimg`)** | Low-level implementation of image processing using `bimg`. | Domain interfaces. |

### Data Flow

1.  **gRPC Request:** User sends image bytes → gRPC handler → Worker Pool → `bimg` process → gRPC response.
2.  **Kafka Event:** Consumer receives "image-uploaded" event → Worker Pool → `bimg` process → Producer sends "image-processed" event.

## Patterns to Follow

### Pattern 1: Bounded Worker Pool
**What:** Use a fixed-size pool of workers to process CPU-intensive image tasks.
**When:** To prevent CPU thrashing and OOM during spikes.
**Example:**
```go
// size to GOMAXPROCS for CPU-bound tasks
numWorkers := runtime.GOMAXPROCS(0)
jobs := make(chan []byte, numWorkers)
results := make(chan []byte, numWorkers)

for i := 0; i < numWorkers; i++ {
    go func() {
        for img := range jobs {
            processed, _ := bimg.Process(img, options)
            results <- processed
        }
    }()
}
```

### Pattern 2: Structured Concurrency
**What:** Manage the lifecycle of gRPC server and Kafka consumer using `errgroup`.
**When:** When running multiple long-running services in a single process.
**Example:**
```go
g, ctx := errgroup.WithContext(ctx)
g.Go(func() error { return grpcServer.Serve(lis) })
g.Go(func() error { return kafkaConsumer.Start(ctx) })
return g.Wait()
```

## Anti-Patterns to Avoid

### Anti-Pattern 1: Disk I/O as Buffer
**What:** Saving images to `/tmp` before processing.
**Why bad:** Significantly slower and causes scaling issues when local storage fills up.
**Instead:** Pass `[]byte` buffers directly between layers.

### Anti-Pattern 2: Unlimited Goroutines
**What:** Spawning a new goroutine for every image request.
**Why bad:** Image processing is CPU-bound. Too many goroutines will cause context switching overhead and memory exhaustion (each `libvips` instance uses memory).
**Instead:** Use a bounded worker pool sized to available CPU cores.

## Scalability Considerations

| Concern | At 100 users | At 10K users | At 1M users |
|---------|--------------|--------------|-------------|
| **CPU Usage** | Single instance | Multiple replicas | Kubernetes HPA based on CPU |
| **Memory** | Single instance | Monitor `libvips` cache | Strict container memory limits |
| **Kafka Lag** | Single partition | Multiple partitions | Dynamic consumer groups |

## Sources

- [Standard Go Project Layout](https://github.com/golang-standards/project-layout)
- [Hexagonal Architecture in Go](https://medium.com/@matiasvarela/hexagonal-architecture-in-go-cfd45869d816)
- [Structured Concurrency in Go](https://sourcegraph.com/blog/concurrency-in-go)
