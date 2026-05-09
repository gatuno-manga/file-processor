package processor

import (
	"bytes"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"math"
	"sync"

	"github.com/bbrks/go-blurhash"
	"github.com/cenkalti/dominantcolor"
	"github.com/corona10/goimagehash"
	"github.com/h2non/bimg"
	_ "golang.org/x/image/webp"
)

func extractMetadata(input []byte) (*Metadata, error) {
	var wg sync.WaitGroup
	var entropy float64
	wg.Add(1)
	go func() {
		defer wg.Done()
		entropy = calculateEntropy(input)
	}()

	img := bimg.NewImage(input)
	imgMeta, err := img.Metadata()
	if err != nil {
		return nil, fmt.Errorf("failed to get image metadata: %w", err)
	}

	// Advanced metadata requires standard image.Image
	// Thumbnail() is much faster as it uses shrink-on-load for JPEG/WebP.
	// We force JPEG with low quality for the thumbnail to minimize encoding/decoding overhead.
	thumbnail, err := img.Process(bimg.Options{
		Width:   64,
		Height:  64,
		Crop:    true,
		Type:    bimg.JPEG,
		Quality: 10,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to generate thumbnail for advanced metadata: %w", err)
	}

	decoded, _, err := image.Decode(bytes.NewReader(thumbnail))
	if err != nil {
		return nil, fmt.Errorf("failed to decode thumbnail for advanced metadata: %w", err)
	}

	meta := &Metadata{
		Width:        imgMeta.Size.Width,
		Height:       imgMeta.Size.Height,
		FormatOrigin: imgMeta.Type,
		MimeType:     "image/" + imgMeta.Type,
	}

	var metaWg sync.WaitGroup
	metaWg.Add(3)

	// Dominant Color
	go func() {
		defer metaWg.Done()
		meta.DominantColor = dominantcolor.Hex(dominantcolor.Find(decoded))
	}()

	// pHash
	go func() {
		defer metaWg.Done()
		hash, err := goimagehash.PerceptionHash(decoded)
		if err == nil {
			meta.PHash = hash.ToString()
		}
	}()

	// BlurHash
	go func() {
		defer metaWg.Done()
		bh, err := blurhash.Encode(4, 3, decoded)
		if err == nil {
			meta.BlurHash = bh
		}
	}()

	metaWg.Wait()
	wg.Wait()
	meta.Entropy = entropy

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
