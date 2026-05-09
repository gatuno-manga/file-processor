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
	_ "golang.org/x/image/webp"
)

func extractMetadata(input []byte) (*Metadata, error) {
	img := bimg.NewImage(input)
	imgMeta, err := img.Metadata()
	if err != nil {
		return nil, fmt.Errorf("failed to get image metadata: %w", err)
	}

	// Basic metadata
	meta := &Metadata{
		Width:        imgMeta.Size.Width,
		Height:       imgMeta.Size.Height,
		FormatOrigin: imgMeta.Type,
		MimeType:     "image/" + imgMeta.Type,
		Entropy:      calculateEntropy(input),
	}

	// Advanced metadata requires standard image.Image
	// We create a small thumbnail using bimg first to avoid decoding large images in pure Go
	thumbnail, err := img.Resize(32, 0)
	if err != nil {
		return meta, fmt.Errorf("failed to generate thumbnail for advanced metadata: %w", err)
	}

	decoded, _, err := image.Decode(bytes.NewReader(thumbnail))
	if err != nil {
		return meta, fmt.Errorf("failed to decode thumbnail for advanced metadata: %w", err)
	}

	bounds := decoded.Bounds()
	if bounds.Dx() <= 0 || bounds.Dy() <= 0 {
		return meta, fmt.Errorf("decoded thumbnail has invalid dimensions: %dx%d", bounds.Dx(), bounds.Dy())
	}

	// Dominant Color
	meta.DominantColor = dominantcolor.Hex(dominantcolor.Find(decoded))

	// pHash
	hash, err := goimagehash.PerceptionHash(decoded)
	if err == nil {
		meta.PHash = hash.ToString()
	}

	// BlurHash (Already using a small decoded image)
	bh, err := blurhash.Encode(4, 3, decoded)
	if err == nil {
		meta.BlurHash = bh
	}

	return meta, nil
}

func calculateEntropy(data []byte) float64 {
	if len(data) == 0 {
		return 0
	}
	var frequencies [256]int
	for _, b := range data {
		frequencies[b]++
	}
	entropy := 0.0
	for _, count := range frequencies {
		if count > 0 {
			p := float64(count) / float64(len(data))
			entropy -= p * math.Log2(p)
		}
	}
	return entropy
}
