package processor

import (
	"errors"
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
)

// Process takes an image byte buffer and returns a metadata-stripped,
// auto-rotated, lossless WebP byte buffer.
func Process(input []byte) ([]byte, error) {
	start := time.Now()
	defer func() {
		processedTotal.Inc()
		processDuration.Observe(time.Since(start).Seconds())
	}()

	if len(input) == 0 {
		return nil, errors.New("input buffer is empty")
	}

	options := bimg.Options{
		Type:          bimg.WEBP,
		Lossless:      true,
		StripMetadata: true,
	}

	output, err := bimg.NewImage(input).Process(options)
	if err != nil {
		return nil, err
	}

	return output, nil
}
