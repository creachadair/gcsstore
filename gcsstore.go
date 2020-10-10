package gcsstore

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/creachadair/ffs/blob"
	"google.golang.org/api/option"
)

// A Store implements the blob.Store interface using a GCS bucket.
type Store struct {
	cli    *storage.Client
	bucket *storage.BucketHandle
}

// New creates a new storage client with the given options.
func New(ctx context.Context, opts Options) (*Store, error) {
	if opts.Bucket == "" {
		return nil, errors.New("missing bucket name")
	}

	var copts []option.ClientOption
	if opts.Credentials != nil {
		bits, err := opts.Credentials(ctx)
		if err != nil {
			return nil, fmt.Errorf("fetching credentials: %w", err)
		}
		copts = append(copts, option.WithCredentialsJSON(bits))
	} else if opts.Unauthenticated {
		copts = append(copts, option.WithoutAuthentication())
	}

	cli, err := storage.NewClient(ctx, copts...)
	if err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}
	bucket := cli.Bucket(opts.Bucket)
	if _, err := bucket.Attrs(ctx); err != nil {
		return nil, fmt.Errorf("bucket %q: %w", opts.Bucket, err)
	}
	return &Store{cli: cli, bucket: bucket}, nil
}

// Options control the construction of a *Store.
type Options struct {
	// The name of the storage bucket to use (required).
	Bucket string

	// If not nil, return JSON credentials.
	Credentials func(context.Context) ([]byte, error)

	// If true and credentials are not provided, connect without authentication.
	// If false, default application credentials will be used from the environment.
	Unauthenticated bool
}

// Getimplements a method of the blob.Store interface.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) { panic("ok") }

// Put implements a method of the blob.Store interface.
func (s *Store) Put(ctx context.Context, opts blob.PutOptions) error { panic("ok") }

// Delete implements a method of the blob.Store interface.
func (s *Store) Delete(ctx context.Context, key string) error { panic("ok") }

// Size implements a method of the blob.Store interface.
func (s *Store) Size(ctx context.Context, key string) (int64, error) { panic("ok") }

// List implements a method of the blob.Store interface.
func (s *Store) List(ctx context.Context, start string, f func(string) error) error { panic("ok") }

// Len implements a method of the blob.Store interface.
func (s *Store) Len(ctx context.Context) (int64, error) { panic("ok") }

// Close closes the client associated with s.
func (s *Store) Close() error { return s.cli.Close() }
