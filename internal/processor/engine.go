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

	img := bimg.NewImage(input)
	metadata, err := extractMetadata(input)
	if err != nil {
		slog.Warn("failed to extract metadata", "error", err)
	}

	// Logic for backfill: if already webp and isBackfill, we can skip processing if desired.
	// However, the mandate says "optimize by extracting metadata and skipping redundant conversion".
	// We'll still want to return the (potentially same) bytes.
	
	if isBackfill && metadata != nil && metadata.MimeType == "image/webp" {
		// Just return original bytes and extracted metadata
		metadata.SizeBytes = len(input)
		return input, metadata, nil
	}

	options := bimg.Options{
		Type:          bimg.WEBP,
		StripMetadata: true,
	}

	if metadata != nil {
		const maxWebPSize = 16383
		if metadata.Width > maxWebPSize || metadata.Height > maxWebPSize {
			slog.Warn("image exceeds WebP limits, keeping original format to avoid resizing", "width", metadata.Width, "height", metadata.Height)
			options.Type = bimg.UNKNOWN
		}
	}

	if quality > 0 {
		options.Quality = quality
		// Dynamic quality adjustment based on entropy
		if quality == DefaultQuality && metadata != nil {
			if metadata.Entropy < 5.0 {
				options.Quality = 90 // Protect smooth gradients/flat areas where artifacts are visible
			} else {
				options.Quality = 80 // Default to 80 for normal/high entropy where masking works
			}
		}
		options.Lossless = false
	} else {
		options.Lossless = true
	}

	output, err := img.Process(options)
	if err != nil {
		return nil, nil, err
	}

	if metadata != nil {
		metadata.SizeBytes = len(output)
		metadata.MimeType = "image/webp"
		if options.Type == bimg.UNKNOWN {
			// If we kept original format
			metadata.MimeType = "image/" + metadata.FormatOrigin
		}
	}

	return output, metadata, nil
}
