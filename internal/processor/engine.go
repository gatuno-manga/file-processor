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

// Process takes an image byte buffer and returns a metadata-stripped,
// auto-rotated WebP byte buffer using DefaultQuality.
func Process(input []byte) ([]byte, error) {
	return ProcessLossy(input, DefaultQuality)
}

// ProcessLossy takes an image byte buffer and returns a metadata-stripped,
// auto-rotated WebP byte buffer with the specified quality (1-100).
// If quality is 0, it uses lossless compression.
func ProcessLossy(input []byte, quality int) ([]byte, error) {
	start := time.Now()
	defer func() {
		processedTotal.Inc()
		processDuration.Observe(time.Since(start).Seconds())
	}()

	if len(input) == 0 {
		return nil, errors.New("input buffer is empty")
	}

	img := bimg.NewImage(input)
	if meta, err := img.Metadata(); err == nil {
		if meta.Type == "webp" && quality == 0 && meta.Orientation == 0 {
		}
	}

	options := bimg.Options{
		Type:          bimg.WEBP,
		StripMetadata: true,
	}

	if size, err := img.Size(); err == nil {
		const maxWebPSize = 16383
		if size.Width > maxWebPSize || size.Height > maxWebPSize {
			slog.Warn("image exceeds WebP limits, keeping original format to avoid resizing", "width", size.Width, "height", size.Height)
			options.Type = bimg.UNKNOWN
		}
	}

	if quality > 0 {
		options.Quality = quality
		options.Lossless = false
	} else {
		options.Lossless = true
	}

	output, err := bimg.NewImage(input).Process(options)
	if err != nil {
		return nil, err
	}

	return output, nil
}
