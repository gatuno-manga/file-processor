package kafka

// ImageDownloadedEvent represents the payload from the image.downloaded topic.
type ImageDownloadedEvent struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

// FileSanitizedEvent represents the payload sent to the file.sanitized topic.
type FileSanitizedEvent struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}
