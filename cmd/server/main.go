package main

import (
	"fmt"
	"github.com/luis/file-processor/internal/processor"
)

func main() {
	fmt.Println("Gatuno File Processor starting...")
	processor.InitVips()
	fmt.Println("Vips initialized successfully.")
}
