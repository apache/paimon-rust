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
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	paimon "github.com/apache/paimon-rust/bindings/go"
)

func blobDescriptorV2(uri string, offset, length int64) []byte {
	result := make([]byte, 0, 29+len(uri))
	result = append(result, 2)
	result = binary.LittleEndian.AppendUint64(result, 0x424C4F4244455343)
	result = binary.LittleEndian.AppendUint32(result, uint32(len(uri)))
	result = append(result, uri...)
	result = binary.LittleEndian.AppendUint64(result, uint64(offset))
	result = binary.LittleEndian.AppendUint64(result, uint64(length))
	return result
}

func localFileURI(path string) string {
	return (&url.URL{Scheme: "file", Path: path}).String()
}

func writeBlobFile(t *testing.T, name, value string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestBlobReaderReadBlobAndBatch(t *testing.T) {
	first := writeBlobFile(t, "first", "abcdefghij")
	second := writeBlobFile(t, "second", "UVWXYZ")

	reader, err := paimon.NewBlobReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	value, err := reader.ReadBlob(blobDescriptorV2(localFileURI(first), 1, 3))
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "bcd" {
		t.Fatalf("ReadBlob returned %q, want %q", value, "bcd")
	}

	values, err := reader.ReadBlobs([][]byte{
		blobDescriptorV2(localFileURI(second), 1, 3),
		blobDescriptorV2(localFileURI(first), 3, -1),
		blobDescriptorV2(localFileURI(first), 5, 0),
		blobDescriptorV2(localFileURI(first), 2, 4),
		blobDescriptorV2(localFileURI(first), 2, 4),
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"VWX", "defghij", "", "cdef", "cdef"}
	for index, value := range values {
		if string(value) != want[index] {
			t.Fatalf("ReadBlobs result %d = %q, want %q", index, value, want[index])
		}
	}

	empty, err := reader.ReadBlobs(nil)
	if err != nil {
		t.Fatal(err)
	}
	if empty == nil || len(empty) != 0 {
		t.Fatalf("empty batch returned %#v", empty)
	}
}

func TestStringBlobMapDescriptors(t *testing.T) {
	source := filepath.Join("testdata", "map_blob_table")
	warehouse := t.TempDir()
	if err := copyDirectory(source, filepath.Join(warehouse, "default.db", "map_blob_table")); err != nil {
		t.Fatal(err)
	}
	table := openTableAt(t, warehouse, "map_blob_table")
	builder, err := table.NewReadBuilderWithOptions(map[string]string{
		"blob-as-descriptor": "true",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer builder.Close()
	if err := builder.WithProjection([]string{"id", "assets"}); err != nil {
		t.Fatal(err)
	}
	scan, err := builder.NewScan()
	if err != nil {
		t.Fatal(err)
	}
	defer scan.Close()
	plan, err := scan.Plan()
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Close()
	read, err := builder.NewRead()
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	batches, err := read.NewRecordBatchReader(plan.Splits())
	if err != nil {
		t.Fatal(err)
	}
	defer batches.Close()

	rows := make(map[int32]map[string][]byte)
	for {
		record, err := batches.NextRecord()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		ids := record.Column(0).(*array.Int32)
		for row := 0; row < int(record.NumRows()); row++ {
			rows[ids.Value(row)], err = paimon.StringBlobMapDescriptors(record.Column(1), row)
			if err != nil {
				record.Release()
				t.Fatal(err)
			}
		}
		record.Release()
	}

	descriptors := rows[1]
	if len(rows) != 3 {
		t.Fatalf("read %d rows, want 3", len(rows))
	}
	if len(descriptors["first"]) == 0 || len(descriptors["tail"]) == 0 {
		t.Fatalf("descriptor map is invalid after Arrow release: %#v", descriptors)
	}
	if rows[2] != nil || len(rows[3]) != 0 {
		t.Fatalf("unexpected null or empty maps: %#v", rows)
	}
	reader, err := table.NewBlobReader()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	resolved, err := reader.ReadBlobs([][]byte{
		descriptors["tail"],
		descriptors["first"],
		descriptors["empty"],
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(resolved[0]) != "ghij" || string(resolved[1]) != "abc" || len(resolved[2]) != 0 {
		t.Fatalf("unexpected values: %q", resolved)
	}
	if descriptors["null"] != nil {
		t.Fatalf("null BLOB returned %#v", descriptors["null"])
	}
}

func TestBlobReaderFromTableOutlivesTable(t *testing.T) {
	file := writeBlobFile(t, "table", "abcdefghij")

	table := openCopiedTestTable(t)
	reader, err := table.NewBlobReader()
	if err != nil {
		t.Fatal(err)
	}
	table.Close()
	defer reader.Close()

	value, err := reader.ReadBlob(blobDescriptorV2(localFileURI(file), 2, 4))
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "cdef" {
		t.Fatalf("ReadBlob returned %q, want %q", value, "cdef")
	}
}

func TestBlobReaderErrorsAndClose(t *testing.T) {
	reader, err := paimon.NewBlobReader(map[string]string{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := reader.ReadBlob(nil); err == nil {
		t.Fatal("expected invalid descriptor error")
	}

	missingURI := localFileURI(t.TempDir() + "/missing.blob")
	_, err = reader.ReadBlobs([][]byte{
		blobDescriptorV2(missingURI, 0, 1),
	})
	if err == nil {
		t.Fatal("expected missing object error")
	}
	if !strings.Contains(err.Error(), "input indices [0]") || !strings.Contains(err.Error(), missingURI) {
		t.Fatalf("error lacks descriptor context: %v", err)
	}

	reader.Close()
	reader.Close()
	if _, err := reader.ReadBlob(blobDescriptorV2(missingURI, 0, 0)); !errors.Is(err, paimon.ErrClosed) {
		t.Fatalf("ReadBlob after Close returned %v, want ErrClosed", err)
	}
	if _, err := reader.ReadBlobs(nil); !errors.Is(err, paimon.ErrClosed) {
		t.Fatalf("ReadBlobs after Close returned %v, want ErrClosed", err)
	}
}

func TestBlobStreamReadsIncrementally(t *testing.T) {
	file := writeBlobFile(t, "stream", "abcdefghij")

	reader, err := paimon.NewBlobReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := reader.OpenBlob(blobDescriptorV2(localFileURI(file), 2, 5))
	if err != nil {
		t.Fatal(err)
	}
	reader.Close()
	if size, err := stream.Seek(0, io.SeekEnd); err != nil || size != 5 {
		t.Fatalf("SeekEnd returned (%d, %v), want (5, nil)", size, err)
	}
	if position, err := stream.Seek(1, io.SeekStart); err != nil || position != 1 {
		t.Fatalf("SeekStart returned (%d, %v), want (1, nil)", position, err)
	}
	var ranged bytes.Buffer
	if _, err := io.CopyN(&ranged, stream, 3); err != nil {
		t.Fatal(err)
	}
	if ranged.String() != "def" {
		t.Fatalf("range returned %q, want %q", ranged.String(), "def")
	}
	if _, err := stream.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}

	buffer := make([]byte, 2)
	var value []byte
	for {
		read, err := stream.Read(buffer)
		value = append(value, buffer[:read]...)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if string(value) != "cdefg" {
		t.Fatalf("stream returned %q, want %q", value, "cdefg")
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Read(buffer); !errors.Is(err, paimon.ErrClosed) {
		t.Fatalf("Read after Close returned %v, want ErrClosed", err)
	}
	if _, err := stream.Seek(0, io.SeekStart); !errors.Is(err, paimon.ErrClosed) {
		t.Fatalf("Seek after Close returned %v, want ErrClosed", err)
	}
}

func TestBlobStreamToEndEmptyAndLazyErrors(t *testing.T) {
	file := writeBlobFile(t, "tail", "abcdefghij")

	reader, err := paimon.NewBlobReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	stream, err := reader.OpenBlob(blobDescriptorV2(localFileURI(file), 4, -1))
	if err != nil {
		t.Fatal(err)
	}
	value, err := io.ReadAll(stream)
	if err != nil {
		t.Fatal(err)
	}
	stream.Close()
	if string(value) != "efghij" {
		t.Fatalf("stream returned %q, want %q", value, "efghij")
	}

	empty, err := reader.OpenBlob(blobDescriptorV2(localFileURI(file), 3, 0))
	if err != nil {
		t.Fatal(err)
	}
	value, err = io.ReadAll(empty)
	if err != nil {
		t.Fatal(err)
	}
	empty.Close()
	if len(value) != 0 {
		t.Fatalf("empty stream returned %q", value)
	}

	missing := localFileURI(t.TempDir() + "/missing.blob")
	lazy, err := reader.OpenBlob(blobDescriptorV2(missing, 0, -1))
	if err != nil {
		t.Fatalf("OpenBlob performed eager I/O: %v", err)
	}
	defer lazy.Close()
	if _, err := lazy.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected missing object error on first Read")
	}

	if _, err := reader.OpenBlob(nil); err == nil {
		t.Fatal("expected invalid descriptor error")
	}
}
