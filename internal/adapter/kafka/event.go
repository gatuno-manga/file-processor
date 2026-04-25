package kafka

// ImageProcessingRequestedEvent represents the payload from the image.processing.requested topic.
type ImageProcessingRequestedEvent struct {
	RawPath      string `json:"rawPath"`
	TargetBucket string `json:"targetBucket"`
	TargetPath   string `json:"targetPath"`
}

// ImageProcessingCompletedEvent represents the payload sent to the image.processing.completed topic.
type ImageProcessingCompletedEvent struct {
	RawPath      string `json:"rawPath"`
	TargetBucket string `json:"targetBucket"`
	TargetPath   string `json:"targetPath"`
}
