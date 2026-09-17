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
	"io"
	"path/filepath"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	paimon "github.com/apache/paimon-rust/bindings/go"
)

type nextResult struct {
	record arrow.Record
	err    error
}

func newRecordBatchReader(t *testing.T) *paimon.RecordBatchReader {
	t.Helper()

	warehouse := t.TempDir()
	source := filepath.Join("testdata", "map_blob_table")
	if err := copyDirectory(source, filepath.Join(warehouse, "default.db", "map_blob_table")); err != nil {
		t.Fatal(err)
	}
	table := openTableAt(t, warehouse, "map_blob_table")
	builder, err := table.NewReadBuilderWithOptions(map[string]string{"blob-as-descriptor": "true"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(builder.Close)
	scan, err := builder.NewScan()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(scan.Close)
	plan, err := scan.Plan()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(plan.Close)
	read, err := builder.NewRead()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(read.Close)
	reader, err := read.NewRecordBatchReader(plan.Splits())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reader.Close)
	return reader
}

func TestRecordBatchReaderConcurrentNext(t *testing.T) {
	reader := newRecordBatchReader(t)
	const workers = 16

	start := make(chan struct{})
	results := make(chan nextResult, workers)
	for range workers {
		go func() {
			<-start
			record, err := reader.NextRecord()
			results <- nextResult{record, err}
		}()
	}
	close(start)

	rows := int64(0)
	for range workers {
		result := <-results
		if result.record != nil {
			rows += result.record.NumRows()
			result.record.Release()
		}
		if result.err != nil && !errors.Is(result.err, io.EOF) {
			t.Fatalf("NextRecord returned %v", result.err)
		}
	}
	if rows != 3 {
		t.Fatalf("read %d rows, want 3", rows)
	}
}

func TestRecordBatchReaderConcurrentNextAndClose(t *testing.T) {
	reader := newRecordBatchReader(t)
	const workers = 16

	start := make(chan struct{})
	results := make(chan nextResult, workers)
	var wg sync.WaitGroup
	wg.Add(workers + 1)
	for range workers {
		go func() {
			defer wg.Done()
			<-start
			record, err := reader.NextRecord()
			results <- nextResult{record, err}
		}()
	}
	go func() {
		defer wg.Done()
		<-start
		reader.Close()
	}()
	close(start)
	wg.Wait()
	close(results)

	for result := range results {
		if result.record != nil {
			result.record.Release()
		}
		if result.err != nil && !errors.Is(result.err, io.EOF) && !errors.Is(result.err, paimon.ErrClosed) {
			t.Fatalf("NextRecord returned %v", result.err)
		}
	}
	if _, err := reader.NextRecord(); !errors.Is(err, paimon.ErrClosed) {
		t.Fatalf("NextRecord after Close returned %v", err)
	}
}
