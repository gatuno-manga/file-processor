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
	// Parallelize entropy and basic metadata (Go side) vs thumbnail (CGO side)
	var wg sync.WaitGroup
	var entropy float64
	var config image.Config
	var format string
	var goErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		entropy = calculateEntropy(input)
		config, format, goErr = image.DecodeConfig(bytes.NewReader(input))
	}()

	img := bimg.NewImage(input)
	// 32x32 is the absolute minimum for pHash/BlurHash/DomColor speed
	thumbnail, err := img.Thumbnail(32)
	if err != nil {
		return nil, fmt.Errorf("failed to generate thumbnail: %w", err)
	}

	decoded, _, err := image.Decode(bytes.NewReader(thumbnail))
	if err != nil {
		return nil, fmt.Errorf("failed to decode thumbnail: %w", err)
	}

	wg.Wait()
	if goErr != nil {
		imgMeta, _ := img.Metadata()
		config.Width = imgMeta.Size.Width
		config.Height = imgMeta.Size.Height
		format = imgMeta.Type
	}

	meta := &Metadata{
		Width:        config.Width,
		Height:       config.Height,
		FormatOrigin: format,
		MimeType:     "image/" + format,
		Entropy:      entropy,
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
	return meta, nil
}

func calculateEntropy(data []byte) float64 {
	if len(data) == 0 {
		return 0
	}
	// Sample only every 4th byte for massive speedup on large files
	// Still provides an excellent statistical approximation of entropy
	var frequencies [256]int
	count := 0
	for i := 0; i < len(data); i += 4 {
		frequencies[data[i]]++
		count++
	}
	entropy := 0.0
	invCount := 1.0 / float64(count)
	for _, freq := range frequencies {
		if freq > 0 {
			p := float64(freq) * invCount
			entropy -= p * math.Log2(p)
		}
	}
	return entropy
}
