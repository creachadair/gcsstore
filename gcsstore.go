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
	"strings"

	"cloud.google.com/go/storage"
	"github.com/creachadair/ffs/blob"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// Opener constructs a Store from an address comprising a GCS bucket name, for
// use with the store package.
func Opener(ctx context.Context, addr string) (blob.Store, error) {
	// TODO: Plumb non-default credential settings.
	bucket, prefix := addr, ""
	if i := strings.Index(addr, "@"); i > 0 {
		prefix, bucket = addr[:i], addr[i+1:]
	}

	return New(ctx, Options{
		Bucket: bucket,
		Prefix: prefix,
	})
}

// A Store implements the blob.Store interface using a GCS bucket.
type Store struct {
	cli    *storage.Client
	bucket *storage.BucketHandle
	prefix string
}

// New creates a new storage client with the given options.
func New(ctx context.Context, opts Options) (*Store, error) {
	if opts.Bucket == "" {
		return nil, errors.New("missing bucket name")
	}
	prefix := opts.Prefix
	if prefix == "" {
		prefix = "blob/"
	} else if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
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

	// Verify that the requested bucket exists, and/or create it if the caller
	// supplied a project in the options.
	bucket := cli.Bucket(opts.Bucket)
	if _, err := bucket.Attrs(ctx); err != nil {
		if err == storage.ErrBucketNotExist && opts.Project != "" {
			err = bucket.Create(ctx, opts.Project, opts.BucketAttrs)
		}
		if err != nil {
			return nil, fmt.Errorf("bucket %q: %w", opts.Bucket, err)
		}
	}
	return &Store{cli: cli, bucket: bucket, prefix: prefix}, nil
}

// Options control the construction of a *Store.
type Options struct {
	// The name of the storage bucket to use (required).
	Bucket string

	// The prefix to prepend to each key written by the store.
	// If unset, it defaults to "blob/".
	Prefix string

	// If set, the bucket will be created in this project if it does not exist.
	Project string

	// If set, options to pass when creating a bucket.
	BucketAttrs *storage.BucketAttrs

	// If not nil, return JSON credentials.
	Credentials func(context.Context) ([]byte, error)

	// If true and credentials are not provided, connect without authentication.
	// If false, default application credentials will be used from the environment.
	Unauthenticated bool
}

// Getimplements a method of the blob.Store interface.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	r, err := s.bucket.Object(s.encodeKey(key)).NewReader(ctx)
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
	obj := s.bucket.Object(s.encodeKey(opts.Key))
	if !opts.Replace {
		obj = obj.If(storage.Conditions{
			DoesNotExist: !opts.Replace,
		})
	}
	w := obj.NewWriter(ctx)
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
	err := s.bucket.Object(s.encodeKey(key)).Delete(ctx)
	if err == storage.ErrObjectNotExist {
		return blob.ErrKeyNotFound
	}
	return err
}

// Size implements a method of the blob.Store interface.
func (s *Store) Size(ctx context.Context, key string) (int64, error) {
	attr, err := s.bucket.Object(s.encodeKey(key)).Attrs(ctx)
	if err == storage.ErrObjectNotExist {
		return 0, blob.ErrKeyNotFound
	} else if err != nil {
		return 0, err
	}
	return attr.Size, nil
}

// List implements a method of the blob.Store interface.
func (s *Store) List(ctx context.Context, start string, f func(string) error) error {
	iter := s.bucket.Objects(ctx, &storage.Query{
		Prefix:      s.prefix,
		StartOffset: s.encodeKey(start),
	})
	for {
		attr, err := iter.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return err
		}
		key, err := s.decodeKey(attr.Name)
		if err == errNotMyKey {
			continue // skip; the bucket may contain unrelated keys
		} else if err != nil {
			return fmt.Errorf("invalid key %q", attr.Name)
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

func (s *Store) encodeKey(key string) string {
	return s.prefix + hex.EncodeToString([]byte(key))
}

var errNotMyKey = errors.New("not a blob key")

func (s *Store) decodeKey(ekey string) (string, error) {
	if !strings.HasPrefix(ekey, s.prefix) {
		return "", errNotMyKey
	}
	key, err := hex.DecodeString(strings.TrimPrefix(ekey, s.prefix))
	if err != nil {
		return "", err
	}
	return string(key), nil
}
