package port

import "context"

// Storage defines the interface for object storage interactions.
type Storage interface {
	// Download retrieves an object from the specified bucket and key as a byte buffer.
	Download(ctx context.Context, bucket, key string) ([]byte, error)
	// Upload stores the given data as an object in the specified bucket and key.
	Upload(ctx context.Context, bucket, key string, data []byte) error
}
