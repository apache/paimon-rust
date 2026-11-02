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
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func testWarehouse(t *testing.T) string {
	t.Helper()
	if warehouse, ok := os.LookupEnv("PAIMON_TEST_WAREHOUSE"); ok {
		return warehouse
	}

	// go test runs each package in its source directory, including with -trimpath.
	archivePath := filepath.Join("..", "..", "..", "crates", "integrations", "datafusion", "testdata", "warehouse", "warehouse.tar.gz")
	file, err := os.Open(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	decoder, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()

	warehouse := t.TempDir()
	archive := tar.NewReader(decoder)
	for {
		entry, err := archive.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if entry.Typeflag != tar.TypeReg || !filepath.IsLocal(entry.Name) {
			t.Fatalf("Unexpected golden archive entry: %s", entry.Name)
		}
		target := filepath.Join(warehouse, filepath.FromSlash(entry.Name))
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			t.Fatal(err)
		}
		output, err := os.Create(target)
		if err != nil {
			t.Fatal(err)
		}
		_, copyErr := io.Copy(output, archive)
		closeErr := output.Close()
		if copyErr != nil {
			t.Fatal(copyErr)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
	}
	return warehouse
}
