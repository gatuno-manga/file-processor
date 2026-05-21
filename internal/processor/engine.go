package processor

import (
	"bytes"
	"errors"
	"fmt"
	"image"
	"log/slog"
	"time"

	"github.com/h2non/bimg"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	processedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "file_processor_processed_total",
		Help: "Total number of processed files",
	}, []string{"type"})

	processDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "file_processor_duration_seconds",
		Help:    "Duration of file processing",
		Buckets: prometheus.DefBuckets,
	}, []string{"type"})
)

// Metadata holds the computed attributes of a processed image.
type Metadata struct {
	Width         int     `json:"width"`
	Height        int     `json:"height"`
	SizeBytes     int     `json:"sizeBytes"`
	MimeType      string  `json:"mimeType"`
	FormatOrigin  string  `json:"formatOrigin"`
	BlurHash      string  `json:"blurHash"`
	DominantColor string  `json:"dominantColor"`
	PHash         string  `json:"pHash"`
	Entropy       float64 `json:"entropy"`
}

// ProcessedResult wraps the output bytes and metadata of a single processed image.
type ProcessedResult struct {
	Data     []byte
	Metadata *Metadata
}

// ImageConfig holds the tunable parameters for image processing.
type ImageConfig struct {
	// Quality is the WebP lossy quality level (1–100). A zero or negative value
	// triggers lossless encoding.
	Quality int
	// MaxHeight is the maximum height (in pixels) before an image is split into
	// vertical slices for processing.
	MaxHeight int
}

// DefaultConfig provides sensible defaults matching the previous behaviour.
var DefaultConfig = ImageConfig{
	Quality:   80,
	MaxHeight: 10000,
}

// Process processes the input image with the default configuration.
func Process(input []byte) ([]ProcessedResult, error) {
	return ProcessLossy(input, DefaultConfig, false)
}


// ProcessLossy converts the input image to WebP using the provided config.
func ProcessLossy(input []byte, cfg ImageConfig, isBackfill bool) ([]ProcessedResult, error) {
	start := time.Now()
	defer func() {
		processedTotal.WithLabelValues("image").Inc()
		processDuration.WithLabelValues("image").Observe(time.Since(start).Seconds())
	}()

	if len(input) == 0 {
		return nil, errors.New("input buffer is empty")
	}

	img := bimg.NewImage(input)
	size, err := img.Size()
	if err != nil {
		return nil, err
	}

	if isBackfill && size.Height <= cfg.MaxHeight {
		_, format, err := image.DecodeConfig(bytes.NewReader(input))
		if err == nil && format == "webp" {
			metadata, err := extractMetadata(input)
			if err == nil {
				metadata.SizeBytes = len(input)
				metadata.MimeType = "image/webp"
				return []ProcessedResult{{Data: input, Metadata: metadata}}, nil
			}
		}
	}

	options := bimg.Options{
		Type:          bimg.WEBP,
		StripMetadata: true,
		Speed:         1,
	}

	if cfg.Quality > 0 {
		options.Quality = cfg.Quality
		options.Lossless = false
	} else {
		options.Lossless = true
	}

	if size.Height <= cfg.MaxHeight {
		output, err := img.Process(options)
		if err != nil {
			return nil, err
		}
		metadata, err := extractMetadata(input)
		if err != nil {
			slog.Warn("failed to extract metadata", "error", err)
		}
		if metadata != nil {
			metadata.SizeBytes = len(output)
			metadata.MimeType = "image/webp"
		}
		return []ProcessedResult{{Data: output, Metadata: metadata}}, nil
	}

	slog.Info("long image detected, splitting", "height", size.Height, "maxHeight", cfg.MaxHeight)
	numParts := (size.Height + cfg.MaxHeight - 1) / cfg.MaxHeight
	results := make([]ProcessedResult, 0, numParts)

	for i := 0; i < numParts; i++ {
		top := i * cfg.MaxHeight
		height := cfg.MaxHeight
		if top+height > size.Height {
			height = size.Height - top
		}

		extractOpts := bimg.Options{
			Top:    top,
			Left:   0,
			Width:  size.Width,
			Height: height,
		}
		slice, err := bimg.NewImage(input).Process(extractOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to extract slice %d: %w", i, err)
		}

		output, err := bimg.NewImage(slice).Process(options)
		if err != nil {
			return nil, fmt.Errorf("failed to process slice %d: %w", i, err)
		}

		metadata, err := extractMetadata(slice)
		if err != nil {
			slog.Warn("failed to extract metadata for slice", "index", i, "error", err)
		}
		if metadata != nil {
			metadata.SizeBytes = len(output)
			metadata.MimeType = "image/webp"
		}
		results = append(results, ProcessedResult{Data: output, Metadata: metadata})
	}

	return results, nil
}
