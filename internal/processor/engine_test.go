package processor

import (
	"encoding/base64"
	"github.com/h2non/bimg"
	"testing"
)

// A minimal 1x1 transparent GIF image
const minimalGif = "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"

func TestProcess(t *testing.T) {
	// Initialize libvips for test
	InitVips()

	// Decode the base64 image
	input, err := base64.StdEncoding.DecodeString(minimalGif)
	if err != nil {
		t.Fatalf("Failed to decode base64 GIF: %v", err)
	}

	// Process the image
	output, err := Process(input)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if len(output) == 0 {
		t.Fatal("Output buffer is empty")
	}

	// Verify it's a valid WebP image
	imageType := bimg.DetermineImageTypeName(output)
	if imageType != "webp" {
		t.Errorf("Expected image type 'webp', but got '%s'", imageType)
	}
}

func TestProcess_EmptyInput(t *testing.T) {
	_, err := Process(nil)
	if err == nil {
		t.Fatal("Expected error for nil input, but got none")
	}
}
