package kafka

// ImageProcessingRequestedEvent represents the payload from the image.processing.requested topic.
type ImageProcessingRequestedEvent struct {
	RawPath      string `json:"rawPath"`
	TargetBucket string `json:"targetBucket"`
	TargetPath   string `json:"targetPath"`
	IsBackfill   bool   `json:"isBackfill"`
}

// ImageProcessingCompletedEvent represents the payload sent to the image.processing.completed topic.
type ImageProcessingCompletedEvent struct {
	RawPath      string              `json:"rawPath"`
	TargetBucket string              `json:"targetBucket"`
	TargetPath   string              `json:"targetPath"`
	Metadata     *MetadataEventField `json:"metadata"`
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
