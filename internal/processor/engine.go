package processor

import (
	"errors"
	"log/slog"
	"time"

	"github.com/h2non/bimg"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	processedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "file_processor_processed_total",
		Help: "The total number of processed images",
	})

	processDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "file_processor_duration_seconds",
		Help:    "Duration of image processing in seconds",
		Buckets: prometheus.DefBuckets,
	})

	DefaultQuality = 80
)

// Metadata contains technical data extracted from an image.
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

// Process takes an image byte buffer and returns a metadata-stripped,
// auto-rotated WebP byte buffer using DefaultQuality and its metadata.
func Process(input []byte) ([]byte, *Metadata, error) {
	return ProcessLossy(input, DefaultQuality, false)
}

// ProcessLossy takes an image byte buffer and returns a metadata-stripped,
// auto-rotated WebP byte buffer with the specified quality (1-100) and its metadata.
// If quality is 0, it uses lossless compression.
// If isBackfill is true and image is already WebP, it might skip conversion.
func ProcessLossy(input []byte, quality int, isBackfill bool) ([]byte, *Metadata, error) {
	start := time.Now()
	defer func() {
		processedTotal.Inc()
		processDuration.Observe(time.Since(start).Seconds())
	}()

	if len(input) == 0 {
		return nil, nil, errors.New("input buffer is empty")
	}

	// For backfill, we can do a fast metadata check first
	if isBackfill {
		img := bimg.NewImage(input)
		imgMeta, err := img.Metadata()
		if err == nil && imgMeta.Type == "webp" {
			metadata, err := extractMetadata(input)
			if err != nil {
				slog.Warn("failed to extract metadata for backfill", "error", err)
			} else {
				metadata.SizeBytes = len(input)
			}
			return input, metadata, nil
		}
	}

	// Entropy calculation is extremely fast (<1ms) and can be done before deciding quality
	// but we'll include it in the concurrent metadata extraction to keep the logic clean
	// and only do one input traversal.

	type metaResult struct {
		meta *Metadata
		err  error
	}
	metaChan := make(chan metaResult, 1)

	// Start metadata extraction in the background
	go func() {
		meta, err := extractMetadata(input)
		metaChan <- metaResult{meta, err}
	}()

	options := bimg.Options{
		Type:          bimg.WEBP,
		StripMetadata: true,
	}

	if quality > 0 {
		options.Quality = quality
		options.Lossless = false
	} else {
		options.Lossless = true
	}

	// Main libvips processing
	img := bimg.NewImage(input)
	output, err := img.Process(options)
	if err != nil {
		return nil, nil, err
	}

	// Wait for metadata extraction (should be finished by now or shortly after)
	res := <-metaChan
	metadata := res.meta
	if res.err != nil {
		slog.Warn("failed to extract metadata", "error", res.err)
	}

	if metadata != nil {
		metadata.SizeBytes = len(output)
		metadata.MimeType = "image/webp"
	}

	return output, metadata, nil
}
