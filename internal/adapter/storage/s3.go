package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 5*1024*1024)
	},
}

type minioClient interface {
	GetObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, error)
	PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (info minio.UploadInfo, err error)
	RemoveObject(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error
	ListBuckets(ctx context.Context) ([]minio.BucketInfo, error)
}

type minioClientImpl struct {
	client *minio.Client
}

func (m *minioClientImpl) GetObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, error) {
	return m.client.GetObject(ctx, bucketName, objectName, opts)
}

func (m *minioClientImpl) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	return m.client.PutObject(ctx, bucketName, objectName, reader, objectSize, opts)
}

func (m *minioClientImpl) RemoveObject(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error {
	return m.client.RemoveObject(ctx, bucketName, objectName, opts)
}

func (m *minioClientImpl) ListBuckets(ctx context.Context) ([]minio.BucketInfo, error) {
	return m.client.ListBuckets(ctx)
}

type S3Adapter struct {
	client minioClient
}

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

func (a *S3Adapter) Ping(ctx context.Context) error {
	_, err := a.client.ListBuckets(ctx)
	if err != nil {
		return fmt.Errorf("s3 connectivity check failed: %w", err)
	}
	return nil
}

func (a *S3Adapter) Download(ctx context.Context, bucket, key string) ([]byte, error) {
	object, err := a.client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get object from s3: %w", err)
	}
	defer object.Close()

	buf := bufferPool.Get().([]byte)
	b := bytes.NewBuffer(buf[:0])

	_, err = io.Copy(b, object)
	if err != nil {
		bufferPool.Put(buf)
		return nil, fmt.Errorf("failed to read object data: %w", err)
	}

	return b.Bytes(), nil
}

func (a *S3Adapter) Upload(ctx context.Context, bucket, key string, data []byte, contentType string) error {
	_, err := a.client.PutObject(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return fmt.Errorf("failed to upload object to s3: %w", err)
	}

	return nil
}

func (a *S3Adapter) Delete(ctx context.Context, bucket, key string) error {
	err := a.client.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete object from s3: %w", err)
	}

	return nil
}

func (a *S3Adapter) Release(data []byte) {
	if cap(data) > 0 {
		bufferPool.Put(data[:0])
	}
}
