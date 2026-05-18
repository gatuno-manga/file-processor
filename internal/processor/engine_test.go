package processor

import (
	"encoding/base64"
	"testing"
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
