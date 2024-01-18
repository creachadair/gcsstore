// Copyright (C) 2020 Michael J. Fromberger. All Rights Reserved.

// Package gcsstore implements the blob.Store interface using a GCS bucket.
package gcsstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/hexkey"
	"github.com/creachadair/taskgroup"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// Opener constructs a Store from an address comprising a GCS bucket name, for
// use with the store package.
//
// The format of addr is "[prefix@]bucket-name[?query]".
//
// Query parameters:
//
//	shard_prefix  : shard prefix length (int)
func Opener(ctx context.Context, addr string) (blob.Store, error) {
	prefix, bucket, ok := strings.Cut(addr, "@")
	if !ok {
		prefix, bucket = bucket, prefix
	}
	opts := Options{Prefix: prefix}
	if base, query, ok := strings.Cut(bucket, "?"); ok {
		bucket = base
		q, err := url.ParseQuery(query)
		if err != nil {
			return nil, fmt.Errorf("invalid query: %w", err)
		}
		if v, ok := getQueryInt(q, "shard_len"); ok {
			opts.ShardPrefixLen = v
		}
	}
	return New(ctx, bucket, opts)
}

// A Store implements the blob.Store interface using a GCS bucket.
type Store struct {
	cli    *storage.Client
	bucket *storage.BucketHandle
	key    hexkey.Config
}

// New creates a new storage client for the given bucket.
func New(ctx context.Context, bucketName string, opts Options) (*Store, error) {
	if bucketName == "" {
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

	// Verify that the requested bucket exists, and/or create it if the caller
	// supplied a project in the options.
	bucket := cli.Bucket(bucketName)
	if _, err := bucket.Attrs(ctx); err != nil {
		if err == storage.ErrBucketNotExist && opts.Project != "" {
			err = bucket.Create(ctx, opts.Project, opts.BucketAttrs)
		}
		if err != nil {
			return nil, fmt.Errorf("bucket %q: %w", bucketName, err)
		}
	}
	return &Store{
		cli:    cli,
		bucket: bucket,
		key:    hexkey.Config{Prefix: opts.Prefix, Shard: opts.ShardPrefixLen},
	}, nil
}

// Options control the construction of a *Store.
type Options struct {
	// The prefix to prepend to each key written by the store.
	// If unset, no prefix is prepended and keys are written at the top level.
	// See also ShardPrefixLen.
	Prefix string

	// The length of the key shard prefix. If positive, the key is partitioned
	// into a prefix of this length and a suffix comprising the rest of the key,
	// separated by a "/". For example, if ShardPrefixLen is 3, then the key
	// 01234567 will besplit to 012/34567.
	ShardPrefixLen int

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

// Get implements a method of the blob.Store interface.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	r, err := s.bucket.Object(s.key.Encode(key)).NewReader(ctx)
	if err == storage.ErrObjectNotExist {
		return nil, blob.KeyNotFound(key)
	} else if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// Put implements a method of the blob.Store interface.
func (s *Store) Put(ctx context.Context, opts blob.PutOptions) error {
	obj := s.bucket.Object(s.key.Encode(opts.Key))
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
			return blob.KeyExists(opts.Key)
		}
		return err
	}
	return nil
}

// Delete implements a method of the blob.Store interface.
func (s *Store) Delete(ctx context.Context, key string) error {
	err := s.bucket.Object(s.key.Encode(key)).Delete(ctx)
	if err == storage.ErrObjectNotExist {
		return blob.KeyNotFound(key)
	}
	return err
}

// List implements a method of the blob.Store interface.
func (s *Store) List(ctx context.Context, start string, f func(string) error) error {
	prefix := s.key.Prefix
	if prefix != "" {
		prefix += "/"
	}
	iter := s.bucket.Objects(ctx, &storage.Query{
		Prefix:      prefix,
		StartOffset: s.key.Encode(start),
	})
	for {
		attr, err := iter.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return err
		}
		key, err := s.key.Decode(attr.Name)
		if errors.Is(err, hexkey.ErrNotMyKey) {
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
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g := taskgroup.New(taskgroup.Trigger(cancel))

	var total int64
	c := taskgroup.NewCollector(func(v int64) { total += v })
	for i := 0; i < 256; i++ {
		pfx := string([]byte{byte(i)})
		g.Go(c.Task(func() (int64, error) {
			var count int64
			err := s.List(ctx, pfx, func(key string) error {
				if !strings.HasPrefix(key, pfx) {
					return blob.ErrStopListing
				}
				count++
				return nil
			})
			return count, err
		}))
	}
	err := g.Wait()
	c.Wait()

	return total, err
}

// Close closes the client associated with s.
func (s *Store) Close(_ context.Context) error { return s.cli.Close() }

func getQueryInt(q url.Values, name string) (int, bool) {
	if !q.Has(name) {
		return 0, false
	} else if v, err := strconv.Atoi(q.Get(name)); err == nil {
		return v, true
	}
	return 0, false
}
