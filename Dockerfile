# Stage 1: Build the Go application
FROM golang:1.23-alpine3.20 AS builder

# Install build dependencies for bimg/libvips
# bimg requires vips-dev, build-base, and pkgconfig
RUN apk add --no-cache \
    vips-dev \
    build-base \
    pkgconfig

WORKDIR /app

# Copy dependency files and download
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Build the application
# We use CGO_ENABLED=1 because bimg is a CGO wrapper around libvips
RUN CGO_ENABLED=1 GOOS=linux go build -v -o server ./cmd/server/main.go

# Stage 2: Final minimal image
FROM alpine:3.20

# Install runtime dependencies for libvips and CA certificates for S3
RUN apk add --no-cache \
    vips \
    ca-certificates

# Create a non-root user for security
RUN adduser -D -u 10001 appuser

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/server .

# Ensure the appuser can execute the binary
RUN chown appuser:appuser /app/server

# Switch to non-root user
USER appuser

# Expose gRPC port and Health/Metrics port
EXPOSE 50051 8081

# Command to run
ENTRYPOINT ["./server"]
