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
	"slices"
	"testing"

	paimon "github.com/apache/paimon-rust/bindings/go"
)

func TestCatalogCreateDatabaseAndTable(t *testing.T) {
	catalog, err := paimon.NewCatalog(map[string]string{"warehouse": t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()

	requireCode := func(err error, code paimon.ErrorCode) {
		t.Helper()
		var paimonErr *paimon.Error
		if !errors.As(err, &paimonErr) || paimonErr.Code() != code {
			t.Fatalf("got error %v, want Paimon error code %d", err, code)
		}
	}
	schema := []byte(`{
		"fields": [
			{"id": 0, "name": "id", "type": "INT"},
			{"id": 1, "name": "name", "type": "STRING"}
		],
		"partitionKeys": [],
		"primaryKeys": ["id"],
		"options": {"bucket": "1", "merge-engine": "partial-update"}
	}`)
	id := paimon.NewIdentifier("demo", "users")
	requireCode(catalog.CreateTable(id, schema, false), paimon.CodeNotFound)
	requireCode(catalog.CreateDatabase("demo", map[string]string{"location": "/unused"}, false), paimon.CodeInvalidInput)
	if err := catalog.CreateDatabase("demo", nil, false); err != nil {
		t.Fatal(err)
	}
	requireCode(catalog.CreateDatabase("demo", nil, false), paimon.CodeAlreadyExist)
	if err := catalog.CreateDatabase("demo", nil, true); err != nil {
		t.Fatal(err)
	}
	databases, err := catalog.ListDatabases()
	if err != nil || !slices.Contains(databases, "demo") {
		t.Fatalf("ListDatabases = %v, %v; want demo", databases, err)
	}

	requireCode(catalog.CreateTable(id, []byte("{"), false), paimon.CodeInvalidInput)
	invalidSchema := []byte(`{
		"fields": [{"id": 0, "name": "id", "type": "INT"}],
		"partitionKeys": [],
		"primaryKeys": ["missing"],
		"options": {"bucket": "1"}
	}`)
	requireCode(catalog.CreateTable(id, invalidSchema, false), paimon.CodeInvalidInput)
	requireCode(getTableError(catalog, id), paimon.CodeNotFound)

	if err := catalog.CreateTable(id, schema, false); err != nil {
		t.Fatal(err)
	}
	requireCode(catalog.CreateTable(id, schema, false), paimon.CodeAlreadyExist)
	if err := catalog.CreateTable(id, schema, true); err != nil {
		t.Fatal(err)
	}

	table, err := catalog.GetTable(id)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()

	builder, err := table.NewWriteBuilder()
	if err != nil {
		t.Fatal(err)
	}
	defer builder.Close()
	writer, err := builder.NewWrite()
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	want := []row{{id: 1, name: "Ada"}}
	record := makeRecord(t, want)
	if err := writer.WriteArrowBatch(record); err != nil {
		record.Release()
		t.Fatal(err)
	}
	record.Release()
	messages, err := writer.PrepareCommit()
	if err != nil {
		t.Fatal(err)
	}
	defer messages.Close()
	commit, err := builder.NewCommit()
	if err != nil {
		t.Fatal(err)
	}
	defer commit.Close()
	if err := commit.Commit(messages); err != nil {
		t.Fatal(err)
	}
	if got := readTableRows(t, table); !slices.Equal(got, want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}

	catalog.Close()
	if err := catalog.CreateDatabase("demo", nil, false); !errors.Is(err, paimon.ErrClosed) {
		t.Fatalf("CreateDatabase on closed catalog = %v, want ErrClosed", err)
	}
	if err := catalog.CreateTable(id, schema, false); !errors.Is(err, paimon.ErrClosed) {
		t.Fatalf("CreateTable on closed catalog = %v, want ErrClosed", err)
	}
}

func getTableError(catalog *paimon.Catalog, id paimon.Identifier) error {
	table, err := catalog.GetTable(id)
	if table != nil {
		table.Close()
	}
	return err
}
