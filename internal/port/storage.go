package port

import "context"

// Storage defines the interface for object storage interactions.
type Storage interface {
	Download(ctx context.Context, bucket, key string) ([]byte, error)
	Upload(ctx context.Context, bucket, key string, data []byte) error
	Delete(ctx context.Context, bucket, key string) error
	Release(data []byte)
}
