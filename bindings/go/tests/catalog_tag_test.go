/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package paimon_test

import (
	"errors"
	"path/filepath"
	"testing"

	paimon "github.com/apache/paimon-rust/bindings/go"
)

func TestCatalogTagLifecycle(t *testing.T) {
	source := filepath.Join("testdata", "map_blob_table")
	warehouse := t.TempDir()
	if err := copyDirectory(source, filepath.Join(warehouse, "default.db", "map_blob_table")); err != nil {
		t.Fatal(err)
	}
	catalog, err := paimon.NewCatalog(map[string]string{"warehouse": warehouse})
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()

	id := paimon.NewIdentifier("default", "map_blob_table")
	table, err := catalog.GetTable(id)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	latest, err := table.LatestSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if latest == nil || latest.ID != 1 {
		t.Fatalf("latest snapshot = %#v", latest)
	}

	if err := catalog.CreateTag(id, "release-1", nil, false); err != nil {
		t.Fatal(err)
	}
	tag, err := catalog.GetTag(id, "release-1")
	if err != nil {
		t.Fatal(err)
	}
	if tag.Name != "release-1" || tag.Snapshot.ID != 1 {
		t.Fatalf("unexpected tag: %#v", tag)
	}
	if tag.Snapshot.CommitKind != paimon.CommitKindAppend {
		t.Fatalf("commit kind = %q, want %q", tag.Snapshot.CommitKind, paimon.CommitKindAppend)
	}
	readBuilder, err := table.NewReadBuilderWithOptions(map[string]string{
		"scan.tag-name": "release-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	readBuilder.Close()

	snapshotID := int64(1)
	if err := catalog.CreateTag(id, "release-explicit", &snapshotID, false); err != nil {
		t.Fatal(err)
	}

	err = catalog.CreateTag(id, "release-1", nil, false)
	var paimonErr *paimon.Error
	if !errors.As(err, &paimonErr) || paimonErr.Code() != paimon.CodeAlreadyExist {
		t.Fatalf("duplicate tag error = %v", err)
	}
	if err := catalog.CreateTag(id, "release-1", nil, true); err != nil {
		t.Fatal(err)
	}

	if err := catalog.DeleteTag(id, "release-1", false); err != nil {
		t.Fatal(err)
	}
	_, err = catalog.GetTag(id, "release-1")
	if !errors.As(err, &paimonErr) || paimonErr.Code() != paimon.CodeNotFound {
		t.Fatalf("missing tag error = %v", err)
	}
	if err := catalog.DeleteTag(id, "release-1", true); err != nil {
		t.Fatal(err)
	}
	if err := catalog.DeleteTag(id, "release-explicit", false); err != nil {
		t.Fatal(err)
	}

	catalog.Close()
	if err := catalog.CreateTag(id, "closed", nil, false); !errors.Is(err, paimon.ErrClosed) {
		t.Fatalf("closed catalog error = %v", err)
	}
}
