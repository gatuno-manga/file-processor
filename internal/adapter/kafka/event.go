package kafka

// ImageProcessingRequestedEvent represents the payload from the image.processing.requested topic.
type ImageProcessingRequestedEvent struct {
	RawBucket    string `json:"rawBucket"`
	RawPath      string `json:"rawPath"`
	OriginalUrl  string `json:"originalUrl"`
	TargetBucket string `json:"targetBucket"`
	TargetPath   string `json:"targetPath"`
	IsBackfill   bool   `json:"isBackfill"`
	// Widths is an optional, opt-in list of additional smaller renditions to
	// generate alongside the primary output (e.g. [320, 640, 1024]). Omitted
	// or empty means no variants. Rejected for images taller than the split
	// threshold — splitting and variants are mutually exclusive.
	Widths []int `json:"widths,omitempty"`
}

// ImageProcessingCompletedEvent represents the payload sent to the image.processing.completed topic.
type ImageProcessingCompletedEvent struct {
	RawPath      string                  `json:"rawPath"`
	OriginalUrl  string                  `json:"originalUrl"`
	TargetBucket string                  `json:"targetBucket"`
	Results      []ImageProcessingResult `json:"results"`
}

type ImageProcessingResult struct {
	TargetPath string `json:"targetPath"`
	// Kind is "original", "part" (a vertical slice of a tall, split image),
	// or "variant" (a smaller rendition requested via widths).
	Kind     string              `json:"kind"`
	Metadata *MetadataEventField `json:"metadata"`
}

type MetadataEventField struct {
	Width         int     `json:"width"`
	Height        int     `json:"height"`
	SizeBytes     int     `json:"sizeBytes"`
	MimeType      string  `json:"mimeType"`
	FormatOrigin  string  `json:"formatOrigin"`
	BlurHash      string  `json:"blurHash"`
	DominantColor string  `json:"dominantColor"`
	PHash         string  `json:"pHash"`
	Entropy       float64 `json:"entropy"`
}

// DocumentProcessingRequestedEvent represents the payload from the document.processing.requested topic.
type DocumentProcessingRequestedEvent struct {
	RawBucket    string `json:"rawBucket"`
	RawPath      string `json:"rawPath"`
	TargetBucket string `json:"targetBucket"`
	TargetPath   string `json:"targetPath"`
	Format       string `json:"format"`
}

// DocumentProcessingCompletedEvent represents the payload sent to the document.processing.completed topic.
type DocumentProcessingCompletedEvent struct {
	RawPath      string            `json:"rawPath"`
	TargetBucket string            `json:"targetBucket"`
	TargetPath   string            `json:"targetPath"`
	Metadata     *DocumentMetadata `json:"metadata"`
}

type DocumentMetadata struct {
	SizeBytes    int  `json:"sizeBytes"`
	PageCount    int  `json:"pageCount"`
	IsLinearized bool `json:"isLinearized"`
}
