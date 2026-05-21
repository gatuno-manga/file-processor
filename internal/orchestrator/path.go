package orchestrator

import (
	"fmt"
	"strings"
)

// parseBucketAndKey resolves the final S3 bucket and object key from a raw path
// and an optional explicit bucket name.
//
// It handles three input formats:
//  1. Full URL: "s3://bucket/path/to/file" with rawBucket set or empty
//  2. Explicit bucket + relative path: rawBucket="mybucket", rawPath="path/to/file"
//  3. Legacy combined path: rawBucket="", rawPath="bucket/path/to/file"
func parseBucketAndKey(rawBucket, rawPath string) (bucket, key string, err error) {
	cleanPath := rawPath

	// Strip any URI scheme (e.g. "s3://", "gs://").
	if idx := strings.Index(cleanPath, "://"); idx != -1 {
		cleanPath = cleanPath[idx+3:]
	}
	cleanPath = strings.TrimLeft(cleanPath, "/")

	bucket = rawBucket
	key = cleanPath

	// If bucket is explicitly provided and the path accidentally includes it as
	// a prefix, trim it to avoid redundant paths like "bucket/bucket/key".
	if bucket != "" && strings.HasPrefix(key, bucket+"/") {
		key = key[len(bucket)+1:]
	}

	// Fallback for backward compatibility where rawPath was "bucket/key".
	if bucket == "" {
		parts := strings.SplitN(cleanPath, "/", 2)
		if len(parts) < 2 {
			return "", "", fmt.Errorf("invalid rawPath format (expected bucket/key): %s", rawPath)
		}
		bucket = parts[0]
		key = parts[1]
	}

	if strings.Contains(bucket, ":") || bucket == "" {
		return "", "", fmt.Errorf("invalid bucket name derived from rawPath: %s", rawPath)
	}

	return bucket, key, nil
}
