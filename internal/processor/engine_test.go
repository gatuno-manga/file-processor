package processor

import (
	"bytes"
	"encoding/base64"
	"image"
	"image/color"
	"image/png"
	"testing"

	"golang.org/x/image/tiff"
)

// A minimal 1x1 transparent GIF image
const minimalGif = "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"

func TestProcess(t *testing.T) {
	InitVips(LoadConfig())

	input, err := base64.StdEncoding.DecodeString(minimalGif)
	if err != nil {
		t.Fatalf("Failed to decode base64 GIF: %v", err)
	}

	results, err := Process(input)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("Results are empty")
	}

	output := results[0].Data
	metadata := results[0].Metadata

	if len(output) == 0 {
		t.Fatal("Output buffer is empty")
	}

	if metadata == nil {
		t.Fatal("Metadata is nil")
	}

	if metadata.FormatOrigin != "gif" {
		t.Errorf("Expected FormatOrigin 'gif', but got '%s'", metadata.FormatOrigin)
	}
}

func TestProcess_EmptyInput(t *testing.T) {
	_, err := Process(nil)
	if err == nil {
		t.Fatal("Expected error for nil input, but got none")
	}
}

// TestProcessLossy_MetadataFailureStillReturnsResult covers C1: when
// extractMetadata fails (here, a TIFF input that libvips happily decodes but
// that Go's standard image package cannot, since only gif/jpeg/png/webp
// decoders are registered), ProcessLossy must still succeed and return a
// non-nil Metadata rather than letting the nil escape to callers that
// dereference it unconditionally.
func TestProcessLossy_MetadataFailureStillReturnsResult(t *testing.T) {
	InitVips(LoadConfig())

	src := image.NewRGBA(image.Rect(0, 0, 8, 8))
	for y := 0; y < 8; y++ {
		for x := 0; x < 8; x++ {
			src.Set(x, y, color.RGBA{R: 200, G: 100, B: 50, A: 255})
		}
	}

	var buf bytes.Buffer
	if err := tiff.Encode(&buf, src, nil); err != nil {
		t.Fatalf("failed to encode synthetic TIFF: %v", err)
	}

	results, err := ProcessLossy(buf.Bytes(), DefaultConfig, false)
	if err != nil {
		t.Fatalf("ProcessLossy failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Metadata == nil {
		t.Fatal("expected non-nil Metadata even when metadata extraction fails")
	}
	if results[0].Metadata.MimeType != "image/webp" {
		t.Errorf("expected MimeType 'image/webp', got %q", results[0].Metadata.MimeType)
	}
	if results[0].Metadata.SizeBytes != len(results[0].Data) {
		t.Errorf("expected SizeBytes %d, got %d", len(results[0].Data), results[0].Metadata.SizeBytes)
	}
}

// TestProcessLossy_SplitsTallImage covers C3: a source taller than MaxHeight
// must be split into correctly cropped vertical slices (using AreaWidth /
// AreaHeight, not the resize-only Width / Height fields), not resized as a
// whole and not corrupted on the first slice.
func TestProcessLossy_SplitsTallImage(t *testing.T) {
	InitVips(LoadConfig())

	const width, height = 100, 250
	red := color.RGBA{R: 255, G: 0, B: 0, A: 255}
	green := color.RGBA{R: 0, G: 255, B: 0, A: 255}
	blue := color.RGBA{R: 0, G: 0, B: 255, A: 255}

	src := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		var c color.RGBA
		switch {
		case y < 100:
			c = red
		case y < 200:
			c = green
		default:
			c = blue
		}
		for x := 0; x < width; x++ {
			src.Set(x, y, c)
		}
	}

	var buf bytes.Buffer
	if err := png.Encode(&buf, src); err != nil {
		t.Fatalf("failed to encode synthetic PNG: %v", err)
	}

	cfg := ImageConfig{Quality: 80, MaxHeight: 100}
	results, err := ProcessLossy(buf.Bytes(), cfg, false)
	if err != nil {
		t.Fatalf("ProcessLossy failed: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	wantDims := [][2]int{{100, 100}, {100, 100}, {100, 50}}
	wantDominant := []color.RGBA{red, green, blue}

	for i, res := range results {
		decoded, _, err := image.Decode(bytes.NewReader(res.Data))
		if err != nil {
			t.Fatalf("slice %d: failed to decode output: %v", i, err)
		}

		bounds := decoded.Bounds()
		gotW, gotH := bounds.Dx(), bounds.Dy()
		if gotW != wantDims[i][0] || gotH != wantDims[i][1] {
			t.Errorf("slice %d: expected dimensions %dx%d, got %dx%d", i, wantDims[i][0], wantDims[i][1], gotW, gotH)
		}

		// Sample the center pixel; a genuine crop keeps each slice's band a
		// solid color, while the pre-fix bug (Width/Height instead of
		// AreaWidth/AreaHeight) resized the whole source into slice 0,
		// blending bands together and misplacing them in later slices.
		cx, cy := bounds.Min.X+gotW/2, bounds.Min.Y+gotH/2
		r, g, b, _ := decoded.At(cx, cy).RGBA()
		r8, g8, b8 := uint8(r>>8), uint8(g>>8), uint8(b>>8)

		want := wantDominant[i]
		switch {
		case want == red && !(r8 > g8+40 && r8 > b8+40):
			t.Errorf("slice %d: expected dominant red at center, got rgb(%d,%d,%d)", i, r8, g8, b8)
		case want == green && !(g8 > r8+40 && g8 > b8+40):
			t.Errorf("slice %d: expected dominant green at center, got rgb(%d,%d,%d)", i, r8, g8, b8)
		case want == blue && !(b8 > r8+40 && b8 > g8+40):
			t.Errorf("slice %d: expected dominant blue at center, got rgb(%d,%d,%d)", i, r8, g8, b8)
		}

		if res.Metadata == nil {
			t.Errorf("slice %d: expected non-nil Metadata", i)
		}
	}
}
