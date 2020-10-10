// Copyright (C) 2020 Michael J. Fromberger. All Rights Reserved.

// Package gcsstore implements the blob.Store interface using a GCS bucket.
package gcsstore

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"cloud.google.com/go/storage"
	"github.com/creachadair/ffs/blob"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
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
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	r, err := s.bucket.Object(encodeKey(key)).NewReader(ctx)
	if err == storage.ErrObjectNotExist {
		return nil, blob.ErrKeyNotFound
	} else if err != nil {
		return nil, err
	}
	defer r.Close()
	return ioutil.ReadAll(r)
}

// Put implements a method of the blob.Store interface.
func (s *Store) Put(ctx context.Context, opts blob.PutOptions) error {
	w := s.bucket.Object(encodeKey(opts.Key)).If(storage.Conditions{
		DoesNotExist: !opts.Replace,
	}).NewWriter(ctx)
	if _, err := w.Write(opts.Data); err != nil {
		w.Close()
		return err
	} else if err := w.Close(); err != nil {
		if e, ok := err.(*googleapi.Error); ok && e.Code == http.StatusPreconditionFailed {
			return blob.ErrKeyExists
		}
		return err
	}
	return nil
}

// Delete implements a method of the blob.Store interface.
func (s *Store) Delete(ctx context.Context, key string) error {
	err := s.bucket.Object(encodeKey(key)).Delete(ctx)
	if err == storage.ErrObjectNotExist {
		return blob.ErrKeyNotFound
	}
	return err
}

// Size implements a method of the blob.Store interface.
func (s *Store) Size(ctx context.Context, key string) (int64, error) {
	attr, err := s.bucket.Object(encodeKey(key)).Attrs(ctx)
	if err == storage.ErrObjectNotExist {
		return 0, blob.ErrKeyNotFound
	}
	return attr.Size, nil
}

// List implements a method of the blob.Store interface.
func (s *Store) List(ctx context.Context, start string, f func(string) error) error {
	iter := s.bucket.Objects(ctx, &storage.Query{
		StartOffset: encodeKey(start),
	})
	for {
		attr, err := iter.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return err
		}
		key, err := decodeKey(attr.Name)
		if err != nil {
			continue // skip; the bucket may contain unrelated keys
		}
		if err := f(key); err == blob.ErrStopListing {
			break
		} else if err != nil {
			return err
		}
	}
	return nil
}

// Len implements a method of the blob.Store interface.
func (s *Store) Len(ctx context.Context) (int64, error) {
	// TODO: Is there a better way to get this information than iterating the
	// whole bucket?

	var n int64
	err := s.List(ctx, "", func(string) error {
		n++
		return nil
	})
	return n, err
}

// Close closes the client associated with s.
func (s *Store) Close() error { return s.cli.Close() }

func encodeKey(key string) string { return hex.EncodeToString([]byte(key)) }

func decodeKey(ekey string) (string, error) {
	key, err := hex.DecodeString(ekey)
	if err != nil {
		return "", err
	}
	return string(key), nil
}
