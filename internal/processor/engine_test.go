package processor

import (
	"bytes"
	"encoding/base64"
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/image/tiff"
)

// readTestImage loads a fixture from the repo-level test-images/ directory,
// which holds real-world manga page scans used across this project's tests
// and benchmarks (see benchmark.sh).
func readTestImage(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "test-images", name))
	if err != nil {
		t.Fatalf("failed to read test image %q: %v", name, err)
	}
	return data
}

const testCoverImage = "hentai-manga-pandemonium-6543285-capitulo-1-img-1.jpg"

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

	results, err := ProcessLossy(buf.Bytes(), DefaultConfig, false, nil)
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
	results, err := ProcessLossy(buf.Bytes(), cfg, false, nil)
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

// TestProcessLossy_GeneratesVariants covers the opt-in multi-resolution
// variants feature (.planning/research/IMPACT-multi-resolution-variants.md):
// requested widths must each produce a correctly-sized, aspect-ratio-preserving
// WebP rendition, tagged KindVariant, alongside the untouched KindOriginal
// primary output — using a real manga page fixture, not a synthetic pattern.
func TestProcessLossy_GeneratesVariants(t *testing.T) {
	InitVips(LoadConfig())

	input := readTestImage(t, testCoverImage)
	origCfg, _, err := image.DecodeConfig(bytes.NewReader(input))
	if err != nil {
		t.Fatalf("failed to read source dimensions: %v", err)
	}

	cfg := ImageConfig{Quality: 80, MaxHeight: 10000}
	widths := []int{600, 300}
	results, err := ProcessLossy(input, cfg, false, widths)
	if err != nil {
		t.Fatalf("ProcessLossy failed: %v", err)
	}
	if len(results) != len(widths)+1 {
		t.Fatalf("expected %d results (1 original + %d variants), got %d", len(widths)+1, len(widths), len(results))
	}

	if results[0].Kind != KindOriginal {
		t.Errorf("expected first result Kind=%q, got %q", KindOriginal, results[0].Kind)
	}

	prevSize := len(results[0].Data)
	for i, w := range widths {
		variant := results[i+1]
		if variant.Kind != KindVariant {
			t.Errorf("variant %d (width %d): expected Kind=%q, got %q", i, w, KindVariant, variant.Kind)
		}

		decoded, _, err := image.Decode(bytes.NewReader(variant.Data))
		if err != nil {
			t.Fatalf("variant %d (width %d): failed to decode output: %v", i, w, err)
		}
		bounds := decoded.Bounds()
		if gotW := bounds.Dx(); gotW != w {
			t.Errorf("variant %d: expected width %d, got %d", i, w, gotW)
		}

		wantH := int(float64(w) * float64(origCfg.Height) / float64(origCfg.Width))
		if gotH := bounds.Dy(); gotH < wantH-2 || gotH > wantH+2 {
			t.Errorf("variant %d: expected height ~%d (aspect ratio preserved), got %d", i, wantH, gotH)
		}

		if variant.Metadata == nil {
			t.Fatalf("variant %d: expected non-nil Metadata", i)
		}
		if variant.Metadata.Width != w {
			t.Errorf("variant %d: expected Metadata.Width %d, got %d", i, w, variant.Metadata.Width)
		}
		if variant.Metadata.SizeBytes != len(variant.Data) {
			t.Errorf("variant %d: expected SizeBytes %d, got %d", i, len(variant.Data), variant.Metadata.SizeBytes)
		}

		// Each smaller rendition must be a genuinely smaller payload than the
		// one before it — guards against generateVariants silently returning
		// an unscaled copy of the source.
		if len(variant.Data) >= prevSize {
			t.Errorf("variant %d: expected byte size smaller than the previous rendition (%d bytes), got %d bytes", i, prevSize, len(variant.Data))
		}
		prevSize = len(variant.Data)
	}
}

// TestProcessLossy_SkipsUpscalingVariant covers the "must not upscale" rule
// from the impact study: a requested width at or above the source width is
// skipped (with a warning), not enlarged.
func TestProcessLossy_SkipsUpscalingVariant(t *testing.T) {
	InitVips(LoadConfig())

	input := readTestImage(t, testCoverImage)
	origCfg, _, err := image.DecodeConfig(bytes.NewReader(input))
	if err != nil {
		t.Fatalf("failed to read source dimensions: %v", err)
	}

	results, err := ProcessLossy(input, DefaultConfig, false, []int{origCfg.Width + 500})
	if err != nil {
		t.Fatalf("ProcessLossy failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected the upscaling width to be skipped, leaving only the original; got %d results", len(results))
	}
	if results[0].Kind != KindOriginal {
		t.Errorf("expected the sole result to be the original, got Kind=%q", results[0].Kind)
	}
}

// TestProcessLossy_RejectsTooManyVariantWidths guards the caller-facing safety
// cap: widths are attacker/bug-controlled input from the Kafka event or gRPC
// request, so an oversized list must fail loudly rather than be silently
// truncated or accepted.
func TestProcessLossy_RejectsTooManyVariantWidths(t *testing.T) {
	InitVips(LoadConfig())

	input, err := base64.StdEncoding.DecodeString(minimalGif)
	if err != nil {
		t.Fatalf("failed to decode base64 GIF: %v", err)
	}

	widths := make([]int, MaxVariantWidths+1)
	for i := range widths {
		widths[i] = 10 * (i + 1)
	}

	_, err = ProcessLossy(input, DefaultConfig, false, widths)
	if err == nil {
		t.Fatal("expected an error when requesting more than MaxVariantWidths widths")
	}
}

// TestProcessLossy_RejectsVariantsForSplitEligibleImage covers the confirmed
// v1 scope decision: tall-image splitting and multi-resolution variants are
// mutually exclusive, enforced explicitly (an error) rather than silently
// ignoring one or combining the two naming schemes.
func TestProcessLossy_RejectsVariantsForSplitEligibleImage(t *testing.T) {
	InitVips(LoadConfig())

	const width, height = 100, 250
	src := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			src.Set(x, y, color.RGBA{R: 255, A: 255})
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, src); err != nil {
		t.Fatalf("failed to encode synthetic PNG: %v", err)
	}

	cfg := ImageConfig{Quality: 80, MaxHeight: 100}
	_, err := ProcessLossy(buf.Bytes(), cfg, false, []int{50})
	if err == nil {
		t.Fatal("expected an error when requesting variants for a split-eligible (tall) image")
	}
}

// TestProcessLossy_BackfillGeneratesVariantsWithoutReencode covers the
// confirmed backfill requirement: for the 600GB backlog, the already-compressed
// WebP original must be returned byte-identical (no re-encode), while variants
// are still generated from it.
func TestProcessLossy_BackfillGeneratesVariantsWithoutReencode(t *testing.T) {
	InitVips(LoadConfig())

	jpg := readTestImage(t, testCoverImage)
	cfg := ImageConfig{Quality: 80, MaxHeight: 10000}

	converted, err := ProcessLossy(jpg, cfg, false, nil)
	if err != nil {
		t.Fatalf("failed to produce a WebP fixture: %v", err)
	}
	if len(converted) != 1 {
		t.Fatalf("expected 1 result producing the WebP fixture, got %d", len(converted))
	}
	webp := converted[0].Data

	results, err := ProcessLossy(webp, cfg, true, []int{400})
	if err != nil {
		t.Fatalf("ProcessLossy (backfill) failed: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 1 original + 1 variant, got %d", len(results))
	}

	if !bytes.Equal(results[0].Data, webp) {
		t.Error("expected the backfilled original to be returned byte-identical (no re-encode)")
	}
	if results[0].Kind != KindOriginal {
		t.Errorf("expected Kind=%q, got %q", KindOriginal, results[0].Kind)
	}

	if results[1].Kind != KindVariant {
		t.Errorf("expected Kind=%q, got %q", KindVariant, results[1].Kind)
	}
	if len(results[1].Data) >= len(webp) {
		t.Error("expected the variant to be a smaller payload than the backfilled original")
	}
}
