package processor

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"
)

type DocumentResult struct {
	Data         []byte
	SizeBytes    int
	PageCount    int
	IsLinearized bool
}

func ProcessDocument(data []byte, format string) (*DocumentResult, error) {
	start := time.Now()
	defer func() {
		processedTotal.WithLabelValues("document").Inc()
		processDuration.WithLabelValues("document").Observe(time.Since(start).Seconds())
	}()

	switch strings.ToUpper(format) {
	case "PDF":
		return processPDF(data)
	case "EPUB":
		return processEPUB(data)
	default:
		return nil, fmt.Errorf("unsupported document format: %s", format)
	}
}

func processPDF(data []byte) (*DocumentResult, error) {
	slog.Info("processing PDF document")
	
	return &DocumentResult{
		Data:         data,
		SizeBytes:    len(data),
		PageCount:    0,
		IsLinearized: true,
	}, nil
}

func processEPUB(data []byte) (*DocumentResult, error) {
	slog.Info("processing EPUB document")
	
	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("failed to open EPUB as zip: %w", err)
	}

	var buf bytes.Buffer
	writer := zip.NewWriter(&buf)

	for _, f := range reader.File {
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}

		w, err := writer.Create(f.Name)
		if err != nil {
			rc.Close()
			return nil, err
		}

		if isImageFile(f.Name) {
			imgData, err := io.ReadAll(rc)
			if err == nil {
				_, _ = w.Write(imgData)
			}
		} else {
			_, _ = io.Copy(w, rc)
		}
		rc.Close()
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	processedData := buf.Bytes()
	return &DocumentResult{
		Data:         processedData,
		SizeBytes:    len(processedData),
		PageCount:    0,
		IsLinearized: false,
	}, nil
}

func isImageFile(name string) bool {
	lower := strings.ToLower(name)
	return strings.HasSuffix(lower, ".jpg") || strings.HasSuffix(lower, ".jpeg") || strings.HasSuffix(lower, ".png")
}
