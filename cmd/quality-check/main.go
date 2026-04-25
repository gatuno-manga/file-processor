package main

import (
	"bytes"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/luis/file-processor/internal/processor"
	_ "golang.org/x/image/webp"
)

func main() {
	inputDir := "/input"
	outputDir := "/output"

	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		os.MkdirAll(outputDir, 0755)
	}

	files, err := ioutil.ReadDir(inputDir)
	if err != nil {
		log.Fatalf("Failed to read input directory: %v", err)
	}

	qualityStr := os.Getenv("QUALITY")
	quality := 0
	if qualityStr != "" {
		fmt.Sscanf(qualityStr, "%d", &quality)
	}

	processor.InitVips(processor.LoadConfig())
	defer processor.ShutdownVips()

	fmt.Println("--- Image Compression Quality Test ---")
	if quality > 0 {
		fmt.Printf("Mode: Lossy (Quality: %d)\n", quality)
	} else {
		fmt.Printf("Mode: Lossless\n")
	}
	fmt.Printf("%-30s | %-10s | %-10s | %-8s | %-8s | %-10s | %-15s | %-7s | %-16s\n", "Filename", "Original", "Compressed", "Savings", "PSNR", "Entropy", "BlurHash", "DomColor", "pHash")
	fmt.Println(string(make([]byte, 160)))

	summaryPath := filepath.Join(outputDir, "SUMMARY.md")
	summaryFile, err := os.Create(summaryPath)
	if err != nil {
		fmt.Printf("Warning: could not create summary file: %v\n", err)
	} else {
		defer summaryFile.Close()
		fmt.Fprintf(summaryFile, "# Quality Test Summary\n\n")
		fmt.Fprintf(summaryFile, "Date: %s\n", time.Now().Format(time.RFC1123))
		fmt.Fprintf(summaryFile, "Mode: %s\n\n", map[bool]string{true: fmt.Sprintf("Lossy (Quality: %d)", quality), false: "Lossless"}[quality > 0])
		fmt.Fprintf(summaryFile, "| Filename | Original | Compressed | Savings | PSNR | Entropy | BlurHash | DomColor | pHash | Duration |\n")
		fmt.Fprintf(summaryFile, "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
	}

	var totalOriginal int64
	var totalCompressed int64
	var totalPSNR float64
	var count int

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		ext := filepath.Ext(file.Name())
		if !isSupported(ext) {
			continue
		}

		inputPath := filepath.Join(inputDir, file.Name())
		
		outputName := file.Name() + ".webp"
		if quality > 0 {
			outputName = fmt.Sprintf("%s-q%d.webp", file.Name(), quality)
		}
		outputPath := filepath.Join(outputDir, outputName)

		data, err := ioutil.ReadFile(inputPath)
		if err != nil {
			fmt.Printf("Error reading %s: %v\n", file.Name(), err)
			continue
		}

		start := time.Now()
		processed, metadata, err := processor.ProcessLossy(data, quality, false)
		duration := time.Since(start).Round(time.Millisecond)

		if err != nil {
			fmt.Printf("Error processing %s: %v\n", file.Name(), err)
			continue
		}

		err = ioutil.WriteFile(outputPath, processed, 0644)
		if err != nil {
			fmt.Printf("Error writing %s: %v\n", file.Name(), err)
			continue
		}

		psnrValue := calculatePSNR(data, processed)

		originalSize := int64(len(data))
		compressedSize := int64(len(processed))
		savings := 100.0 - (float64(compressedSize) / float64(originalSize) * 100.0)

		totalOriginal += originalSize
		totalCompressed += compressedSize
		if psnrValue != math.Inf(1) && psnrValue > 0 {
			totalPSNR += psnrValue
		}
		count++

		psnrStr := fmt.Sprintf("%.2fdB", psnrValue)
		if psnrValue == math.Inf(1) {
			psnrStr = "Perfect"
		}

		entropy := 0.0
		blurHash := ""
		dominantColor := ""
		pHash := ""
		if metadata != nil {
			entropy = metadata.Entropy
			blurHash = metadata.BlurHash
			dominantColor = metadata.DominantColor
			pHash = metadata.PHash
		}

		fmt.Printf("%-30s | %-10s | %-10s | %-7.2f%% | %-8s | Ent: %-5.2f | BH: %-15s | DC: %-7s | pH: %-16s | (%v)\n", 
			file.Name(), 
			formatSize(int(originalSize)), 
			formatSize(int(compressedSize)), 
			savings,
			psnrStr,
			entropy,
			blurHash,
			dominantColor,
			pHash,
			duration)

		if summaryFile != nil {
			fmt.Fprintf(summaryFile, "| %s | %s | %s | %.2f%% | %s | %.2f | %s | %s | %s | %v |\n",
				file.Name(),
				formatSize(int(originalSize)),
				formatSize(int(compressedSize)),
				savings,
				psnrStr,
				entropy,
				blurHash,
				dominantColor,
				pHash,
				duration)
		}
	}

	if count > 0 {
		totalSavings := 100.0 - (float64(totalCompressed) / float64(totalOriginal) * 100.0)
		avgPSNR := totalPSNR / float64(count)
		fmt.Println(string(make([]byte, 85)))
		fmt.Printf("%-30s | %-10s | %-10s | %-7.2f%% | %-8.2fdB\n", 
			"TOTAL", 
			formatSize(int(totalOriginal)), 
			formatSize(int(totalCompressed)), 
			totalSavings,
			avgPSNR)
		fmt.Printf("\nTotal space saved: %s\n", formatSize(int(totalOriginal-totalCompressed)))
		fmt.Printf("Total files processed: %d\n", count)
		fmt.Printf("Average PSNR: %.2f dB\n", avgPSNR)

		if summaryFile != nil {
			fmt.Fprintf(summaryFile, "\n## Totals\n\n")
			fmt.Fprintf(summaryFile, "- **Total Original Size:** %s\n", formatSize(int(totalOriginal)))
			fmt.Fprintf(summaryFile, "- **Total Compressed Size:** %s\n", formatSize(int(totalCompressed)))
			fmt.Fprintf(summaryFile, "- **Overall Savings:** %.2f%%\n", totalSavings)
			fmt.Fprintf(summaryFile, "- **Average PSNR:** %.2f dB\n", avgPSNR)
			fmt.Fprintf(summaryFile, "- **Total Space Saved:** %s\n", formatSize(int(totalOriginal-totalCompressed)))
			fmt.Fprintf(summaryFile, "- **Files Processed:** %d\n", count)
		}
	} else {
		fmt.Println("\nNo supported image files found in input directory.")
	}
}

func calculatePSNR(originalData, processedData []byte) float64 {
	img1, _, err := image.Decode(bytes.NewReader(originalData))
	if err != nil {
		return 0
	}
	img2, _, err := image.Decode(bytes.NewReader(processedData))
	if err != nil {
		return 0
	}

	bounds := img1.Bounds()
	if bounds != img2.Bounds() {
		return 0
	}

	var sumSquaredError float64
	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			r1, g1, b1, _ := img1.At(x, y).RGBA()
			r2, g2, b2, _ := img2.At(x, y).RGBA()

			dr := float64(r1>>8) - float64(r2>>8)
			dg := float64(g1>>8) - float64(g2>>8)
			db := float64(b1>>8) - float64(b2>>8)

			sumSquaredError += dr*dr + dg*dg + db*db
		}
	}

	mse := sumSquaredError / float64(bounds.Dx()*bounds.Dy()*3)
	if mse == 0 {
		return math.Inf(1)
	}

	return 20 * math.Log10(255) - 10*math.Log10(mse)
}

func isSupported(ext string) bool {
	switch ext {
	case ".jpg", ".jpeg", ".png", ".gif", ".webp", ".tiff":
		return true
	}
	return false
}

func formatSize(bytes int) string {
	if bytes < 1024 {
		return fmt.Sprintf("%d B", bytes)
	}
	if bytes < 1024*1024 {
		return fmt.Sprintf("%.2f KB", float64(bytes)/1024)
	}
	return fmt.Sprintf("%.2f MB", float64(bytes)/(1024*1024))
}
