package processor

import (
	"fmt"
	"github.com/h2non/bimg"
)

// InitVips initializes the libvips engine with specific cache limits.
func InitVips() {
	// Set libvips cache limits to minimal to ensure predictable memory usage.
	// 0 means disabled/minimal.
	bimg.VipsCacheSetMax(0)
	bimg.VipsCacheSetMaxMem(0)

	fmt.Printf("libvips version: %s\n", bimg.VipsVersion)
}
