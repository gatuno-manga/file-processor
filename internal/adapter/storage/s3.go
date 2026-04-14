package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// minioClient defines the subset of minio.Client methods used by the adapter, returning io.ReadCloser for testability.
type minioClient interface {
	GetObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, error)
	PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (info minio.UploadInfo, err error)
}

// minioClientImpl wraps the real minio.Client.
type minioClientImpl struct {
	client *minio.Client
}

func (m *minioClientImpl) GetObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, error) {
	return m.client.GetObject(ctx, bucketName, objectName, opts)
}

func (m *minioClientImpl) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	return m.client.PutObject(ctx, bucketName, objectName, reader, objectSize, opts)
}

// S3Adapter implements the Storage port for S3-compatible storage.
type S3Adapter struct {
	client minioClient
}

// NewS3Adapter creates a new instance of S3Adapter.
func NewS3Adapter(endpoint, accessKey, secretKey string, useSSL bool) (*S3Adapter, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	return &S3Adapter{client: &minioClientImpl{client: client}}, nil
}

// Download retrieves an object from the specified bucket and key as a byte buffer.
func (a *S3Adapter) Download(ctx context.Context, bucket, key string) ([]byte, error) {
	object, err := a.client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get object from s3: %w", err)
	}
	defer object.Close()

	data, err := io.ReadAll(object)
	if err != nil {
		return nil, fmt.Errorf("failed to read object data: %w", err)
	}

	return data, nil
}

// Upload stores the given data as an object in the specified bucket and key.
func (a *S3Adapter) Upload(ctx context.Context, bucket, key string, data []byte) error {
	_, err := a.client.PutObject(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType: "image/webp",
	})
	if err != nil {
		return fmt.Errorf("failed to upload object to s3: %w", err)
	}

	return nil
}
