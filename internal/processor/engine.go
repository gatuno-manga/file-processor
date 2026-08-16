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

// Result kinds distinguish a full-resolution/original output from a smaller
// derived rendition, so callers never have to guess what a given
// ProcessedResult represents.
const (
	KindOriginal = "original"
	KindPart     = "part"
	KindVariant  = "variant"
)

// MaxVariantWidths bounds how many smaller renditions a single request may ask
// for, independent of caller trust, to keep worst-case CPU/memory cost bounded.
const MaxVariantWidths = 5

// ProcessedResult wraps the output bytes and metadata of a single processed image.
type ProcessedResult struct {
	Data     []byte
	Metadata *Metadata
	// Kind is one of KindOriginal, KindPart (a vertical slice of a tall,
	// split image) or KindVariant (a smaller rendition requested via widths).
	Kind string
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
	return ProcessLossy(input, DefaultConfig, false, nil)
}

// ProcessLossy converts the input image to WebP using the provided config.
// widths is an optional, caller-supplied list of additional smaller renditions
// to generate alongside the primary output (opt-in "variants" feature). It is
// mutually exclusive with tall-image splitting: a source taller than
// cfg.MaxHeight rejects a non-empty widths list rather than silently ignoring
// it or combining the two schemes.
func ProcessLossy(input []byte, cfg ImageConfig, isBackfill bool, widths []int) ([]ProcessedResult, error) {
	start := time.Now()
	defer func() {
		processedTotal.WithLabelValues("image").Inc()
		processDuration.WithLabelValues("image").Observe(time.Since(start).Seconds())
	}()

	if len(input) == 0 {
		return nil, errors.New("input buffer is empty")
	}

	if len(widths) > MaxVariantWidths {
		return nil, fmt.Errorf("too many variant widths requested: %d (max %d)", len(widths), MaxVariantWidths)
	}

	img := bimg.NewImage(input)
	size, err := img.Size()
	if err != nil {
		return nil, err
	}

	if len(widths) > 0 && size.Height > cfg.MaxHeight {
		return nil, fmt.Errorf("variant widths are not supported for images taller than %d (got %d): split and variants are mutually exclusive", cfg.MaxHeight, size.Height)
	}

	if isBackfill && size.Height <= cfg.MaxHeight {
		_, format, err := image.DecodeConfig(bytes.NewReader(input))
		if err == nil && format == "webp" {
			metadata, err := extractMetadata(input)
			if err == nil {
				metadata.SizeBytes = len(input)
				metadata.MimeType = "image/webp"
				variants, err := generateVariants(input, size.Width, cfg, widths)
				if err != nil {
					return nil, err
				}
				results := append([]ProcessedResult{{Data: input, Metadata: metadata, Kind: KindOriginal}}, variants...)
				return results, nil
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
			slog.Warn("metadata extraction failed, emitting partial metadata", "error", err)
			metadata = &Metadata{}
		}
		metadata.SizeBytes = len(output)
		metadata.MimeType = "image/webp"
		variants, err := generateVariants(input, size.Width, cfg, widths)
		if err != nil {
			return nil, err
		}
		results := append([]ProcessedResult{{Data: output, Metadata: metadata, Kind: KindOriginal}}, variants...)
		return results, nil
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
			Top:        top,
			Left:       0,
			AreaWidth:  size.Width,
			AreaHeight: height,
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
			slog.Warn("metadata extraction failed for slice, emitting partial metadata", "index", i, "error", err)
			metadata = &Metadata{}
		}
		metadata.SizeBytes = len(output)
		metadata.MimeType = "image/webp"
		results = append(results, ProcessedResult{Data: output, Metadata: metadata, Kind: KindPart})
	}

	return results, nil
}

// generateVariants renders one smaller WebP rendition per requested width,
// skipping (with a warning, not an error) any width that would upscale the
// source rather than downscale it. Widths are resized from source, not from
// an already-encoded output, so a variant never compounds a second lossy
// re-encode on top of the primary conversion.
func generateVariants(source []byte, sourceWidth int, cfg ImageConfig, widths []int) ([]ProcessedResult, error) {
	if len(widths) == 0 {
		return nil, nil
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

	variants := make([]ProcessedResult, 0, len(widths))
	for _, w := range widths {
		if w <= 0 {
			return nil, fmt.Errorf("invalid variant width: %d", w)
		}
		if w >= sourceWidth {
			slog.Warn("skipping variant width: not smaller than source width", "width", w, "sourceWidth", sourceWidth)
			continue
		}

		variantOptions := options
		variantOptions.Width = w
		output, err := bimg.NewImage(source).Process(variantOptions)
		if err != nil {
			return nil, fmt.Errorf("failed to generate %dw variant: %w", w, err)
		}

		metadata, err := extractMetadata(output)
		if err != nil {
			slog.Warn("metadata extraction failed for variant, emitting partial metadata", "width", w, "error", err)
			metadata = &Metadata{Width: w}
		}
		metadata.SizeBytes = len(output)
		metadata.MimeType = "image/webp"
		variants = append(variants, ProcessedResult{Data: output, Metadata: metadata, Kind: KindVariant})
	}

	return variants, nil
}
