package processor

import (
	"errors"
	"github.com/h2non/bimg"
)

// Process takes an image byte buffer and returns a metadata-stripped,
// auto-rotated, lossless WebP byte buffer.
func Process(input []byte) ([]byte, error) {
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
