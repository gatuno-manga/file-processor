package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/minio/minio-go/v7"
)

type mockMinioClient struct {
	getObjectFunc    func(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, error)
	putObjectFunc    func(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error)
	removeObjectFunc func(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error
	listBucketsFunc  func(ctx context.Context) ([]minio.BucketInfo, error)
}

func (m *mockMinioClient) GetObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, error) {
	if m.getObjectFunc != nil {
		return m.getObjectFunc(ctx, bucketName, objectName, opts)
	}
	return nil, nil
}

func (m *mockMinioClient) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	if m.putObjectFunc != nil {
		return m.putObjectFunc(ctx, bucketName, objectName, reader, objectSize, opts)
	}
	return minio.UploadInfo{}, nil
}

func (m *mockMinioClient) RemoveObject(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error {
	if m.removeObjectFunc != nil {
		return m.removeObjectFunc(ctx, bucketName, objectName, opts)
	}
	return nil
}

func (m *mockMinioClient) ListBuckets(ctx context.Context) ([]minio.BucketInfo, error) {
	if m.listBucketsFunc != nil {
		return m.listBucketsFunc(ctx)
	}
	return nil, nil
}

func TestS3Adapter_Ping(t *testing.T) {
	mc := &mockMinioClient{
		listBucketsFunc: func(ctx context.Context) ([]minio.BucketInfo, error) {
			return []minio.BucketInfo{}, nil
		},
	}
	adapter := &S3Adapter{client: mc}
	if err := adapter.Ping(context.Background()); err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestS3Adapter_Download(t *testing.T) {
	data := []byte("test data")
	mc := &mockMinioClient{
		getObjectFunc: func(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, error) {
			if bucketName != "test-bucket" || objectName != "test-key" {
				return nil, fmt.Errorf("unexpected arguments")
			}
			return io.NopCloser(bytes.NewReader(data)), nil
		},
	}

	adapter := &S3Adapter{client: mc}
	result, err := adapter.Download(context.Background(), "test-bucket", "test-key")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if !bytes.Equal(result, data) {
		t.Errorf("expected %s, got %s", data, result)
	}
}

func TestS3Adapter_Upload(t *testing.T) {
	data := []byte("test data")
	mc := &mockMinioClient{
		putObjectFunc: func(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
			if bucketName != "test-bucket" || objectName != "test-key" {
				return minio.UploadInfo{}, fmt.Errorf("unexpected arguments")
			}
			if objectSize != int64(len(data)) {
				return minio.UploadInfo{}, fmt.Errorf("unexpected size")
			}
			uploaded, _ := io.ReadAll(reader)
			if !bytes.Equal(uploaded, data) {
				return minio.UploadInfo{}, fmt.Errorf("unexpected data")
			}
			return minio.UploadInfo{}, nil
		},
	}

	adapter := &S3Adapter{client: mc}
	err := adapter.Upload(context.Background(), "test-bucket", "test-key", data)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestS3Adapter_Delete(t *testing.T) {
	mc := &mockMinioClient{
		removeObjectFunc: func(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error {
			if bucketName != "test-bucket" || objectName != "test-key" {
				return fmt.Errorf("unexpected arguments")
			}
			return nil
		},
	}

	adapter := &S3Adapter{client: mc}
	err := adapter.Delete(context.Background(), "test-bucket", "test-key")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestS3Adapter_Release(t *testing.T) {
	adapter := &S3Adapter{}
	data := make([]byte, 10)
	adapter.Release(data)
}
