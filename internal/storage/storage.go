// Package storage provides S3 object operations.
package storage

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// Config selects the bucket and, for local development, an S3-compatible endpoint.
type Config struct {
	Region   string
	Endpoint string // empty uses the AWS default
	Bucket   string
}

// Store reads and writes objects in a single bucket.
type Store struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
}

// New builds a Store. Credentials come from the SDK default chain.
func New(ctx context.Context, cfg Config) (*Store, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
		// Objects uploaded in parts have no full-object checksum; skip the per-Get warning.
		o.DisableLogOutputChecksumValidationSkipped = true
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})
	return &Store{client: client, uploader: manager.NewUploader(client), bucket: cfg.Bucket}, nil
}

// EnsureBucket creates the bucket when it does not exist.
func (s *Store) EnsureBucket(ctx context.Context) error {
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &s.bucket})
	if err == nil {
		return nil
	}
	var notFound *types.NotFound
	var apiErr smithy.APIError
	if !errors.As(err, &notFound) && !(errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchBucket") {
		return fmt.Errorf("check bucket %q: %w", s.bucket, err)
	}
	if _, err := s.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &s.bucket}); err != nil {
		var owned *types.BucketAlreadyOwnedByYou
		if errors.As(err, &owned) {
			return nil
		}
		return fmt.Errorf("create bucket %q: %w", s.bucket, err)
	}
	return nil
}

// Put streams r to key using multipart upload without buffering the whole body.
func (s *Store) Put(ctx context.Context, key string, r io.Reader) error {
	_, err := s.uploader.Upload(ctx, &s3.PutObjectInput{Bucket: &s.bucket, Key: &key, Body: r})
	if err != nil {
		return fmt.Errorf("put object %q: %w", key, err)
	}
	return nil
}

// Get returns a streaming reader for key. The caller must close it.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		return nil, fmt.Errorf("get object %q: %w", key, err)
	}
	return out.Body, nil
}

// Delete removes key. Deleting a missing key is not an error.
func (s *Store) Delete(ctx context.Context, key string) error {
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: &key}); err != nil {
		return fmt.Errorf("delete object %q: %w", key, err)
	}
	return nil
}
