// Copyright (C) 2020 Michael J. Fromberger. All Rights Reserved.

package gcsstore_test

import (
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/gcsstore"
)

var (
	credFile = flag.String("credentials", "",
		"Path of JSON credentials file for manual testing")
	bucketName = flag.String("bucket", "gcsstore-test-data",
		"Bucket name to use or create for testing")
)

func TestBasic(t *testing.T) {
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

	t.Logf("Creating client for project %q, bucket %q", info.ProjectID, *bucketName)
	ctx := context.Background()
	s, err := gcsstore.New(ctx, gcsstore.Options{
		Bucket:  *bucketName,
		Project: info.ProjectID,
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
	defer s.Close()

	storetest.Run(t, s)
}
