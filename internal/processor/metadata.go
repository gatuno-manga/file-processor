package processor

import (
	"bytes"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"math"

	"github.com/bbrks/go-blurhash"
	"github.com/cenkalti/dominantcolor"
	"github.com/corona10/goimagehash"
	"github.com/h2non/bimg"
	"github.com/nfnt/resize"
	_ "golang.org/x/image/webp"
)

func extractMetadata(input []byte) (*Metadata, error) {
	img := bimg.NewImage(input)
	size, err := img.Size()
	if err != nil {
		return nil, fmt.Errorf("failed to get image size: %w", err)
	}

	// Basic metadata
	meta := &Metadata{
		Width:        size.Width,
		Height:       size.Height,
		FormatOrigin: img.Type(),
		MimeType:     "image/" + img.Type(),
		Entropy:      calculateEntropy(input),
	}

	// Advanced metadata requires standard image.Image
	decoded, _, err := image.Decode(bytes.NewReader(input))
	if err != nil {
		return meta, fmt.Errorf("failed to decode image for advanced metadata: %w", err)
	}

	// Dominant Color
	meta.DominantColor = dominantcolor.Hex(dominantcolor.Find(decoded))

	// pHash
	hash, err := goimagehash.PerceptionHash(decoded)
	if err == nil {
		meta.PHash = hash.ToString()
	}

	// BlurHash (Downscale for performance)
	// Recommended size for BlurHash is small
	smallImg := resize.Resize(32, 0, decoded, resize.Bilinear)
	bh, err := blurhash.Encode(4, 3, smallImg)
	if err == nil {
		meta.BlurHash = bh
	}

	return meta, nil
}

func calculateEntropy(data []byte) float64 {
	if len(data) == 0 {
		return 0
	}
	frequencies := make(map[byte]int)
	for _, b := range data {
		frequencies[b]++
	}
	entropy := 0.0
	for _, count := range frequencies {
		p := float64(count) / float64(len(data))
		entropy -= p * math.Log2(p)
	}
	return entropy
}
