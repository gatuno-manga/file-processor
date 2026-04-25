#!/bin/bash

# Ensure directories exist
mkdir -p test-images test-results

# Build the test image using the builder stage
docker build -t quality-checker -f - . <<EOF
FROM golang:1.23-alpine3.20 as builder
RUN apk add --no-cache vips-dev build-base pkgconfig
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -o quality-check ./cmd/quality-check/main.go
ENTRYPOINT ["./quality-check"]
EOF

# Run the quality check
# Default quality is lossless (0)
QUALITY=${1:-0}

docker run --rm \
  -e QUALITY=$QUALITY \
  -v "$(pwd)/test-images:/input:ro" \
  -v "$(pwd)/test-results:/output" \
  quality-checker
