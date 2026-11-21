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
	"slices"
	"testing"

	paimon "github.com/apache/paimon-rust/bindings/go"
)

func TestCatalogDropTable(t *testing.T) {
	warehouse := t.TempDir()
	const tableName = "map_blob_table"
	if err := copyDirectory(filepath.Join("testdata", tableName), filepath.Join(warehouse, "default.db", tableName)); err != nil {
		t.Fatal(err)
	}
	catalog, err := paimon.NewCatalog(map[string]string{"warehouse": warehouse})
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()

	id := paimon.NewIdentifier("default", tableName)
	if err := catalog.DropTable(id, false); err != nil {
		t.Fatal(err)
	}
	tables, err := catalog.ListTables("default")
	if err != nil || slices.Contains(tables, tableName) {
		t.Fatalf("ListTables = %v, %v; want %s absent", tables, err, tableName)
	}
	var paimonErr *paimon.Error
	if err := catalog.DropTable(id, false); !errors.As(err, &paimonErr) || paimonErr.Code() != paimon.CodeNotFound {
		t.Fatalf("dropping a missing table returned %v; want CodeNotFound", err)
	}
	if err := catalog.DropTable(id, true); err != nil {
		t.Fatalf("ignoring a missing table returned %v", err)
	}
	catalog.Close()
	if err := catalog.DropTable(id, false); !errors.Is(err, paimon.ErrClosed) {
		t.Fatalf("dropping with a closed catalog returned %v; want ErrClosed", err)
	}
}
