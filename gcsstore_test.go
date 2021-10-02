// Copyright (C) 2020 Michael J. Fromberger. All Rights Reserved.

package gcsstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io/ioutil"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/gcsstore"
)

var (
	credFile = flag.String("credentials", "",
		"Path of JSON credentials file for manual testing")
	bucketName = flag.String("bucket", "gcsstore-test-data",
		"Bucket name to use or create for testing")
)

func credentialsOrSkip(t *testing.T) (creds []byte, projectID string) {
	t.Helper()
	if *credFile == "" {
		t.Skip("Skipping test because -credentials are not set")
	}

	data, err := ioutil.ReadFile(*credFile)
	if err != nil {
		t.Fatalf("Reading credentials: %v", err)
	}
	var info struct {
		ProjectID string `json:"project_id"`
	}
	if err := json.Unmarshal(data, &info); err != nil {
		t.Fatalf("Decoding credentials: %v", err)
	}
	return data, info.ProjectID
}

func storeOrSkip(t *testing.T, prefix string) *gcsstore.Store {
	t.Helper()
	data, projectID := credentialsOrSkip(t)

	t.Logf("Creating client for project %q, bucket %q", projectID, *bucketName)
	ctx := context.Background()
	s, err := gcsstore.New(ctx, *bucketName, gcsstore.Options{
		Prefix:  prefix,
		Project: projectID,
		BucketAttrs: &storage.BucketAttrs{
			StorageClass: "STANDARD",
			Location:     "us-west1",
		},
		Credentials: func(context.Context) ([]byte, error) {
			return data, nil
		},
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	return s
}

func TestProbe(t *testing.T) {
	s := storeOrSkip(t, "testprobe")
	defer s.Close()

	err := s.Put(context.Background(), blob.PutOptions{
		Key:     "test probe key",
		Data:    []byte("This is a blob to manually verify the store settings.\n"),
		Replace: false,
	})
	if errors.Is(err, blob.ErrKeyExists) {
		t.Logf("Put failed: %v", err)
	} else if err != nil {
		t.Errorf("Put failed: %v", err)
	}
}

func TestStoreManual(t *testing.T) {
	s := storeOrSkip(t, "testdata")
	defer s.Close()

	start := time.Now()
	storetest.Run(t, s)
	t.Logf("Store tests completed in %v", time.Since(start))
}
