# Stage 1: Build the Go application
FROM golang:1.23-alpine3.20 AS builder

# Install build dependencies for bimg/libvips
RUN apk add --no-cache vips-dev build-base pkgconfig

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
FROM alpine:latest

# Install runtime dependencies for libvips
RUN apk add --no-cache vips ca-certificates

WORKDIR /root/

# Copy the binary from the builder stage
COPY --from=builder /app/server .

# Expose the port (placeholder for now)
EXPOSE 8080

# Command to run
CMD ["./server"]
