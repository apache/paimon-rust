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
	"os"
	"path/filepath"
	"strings"
	"testing"

	paimon "github.com/apache/paimon-rust/bindings/go"
)

func requireResourceExhausted(t *testing.T, err error) {
	t.Helper()
	var nativeErr *paimon.Error
	if !errors.As(err, &nativeErr) || nativeErr.Code() != paimon.CodeResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got %v", err)
	}
}

func TestResourceContextMetricsAndClose(t *testing.T) {
	resources, err := paimon.NewResourceContext(0)
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := resources.Metrics()
	if err != nil {
		t.Fatal(err)
	}
	if metrics.ReservedMemoryBytes != 0 || metrics.PeakReservedMemoryBytes != 0 {
		t.Fatalf("unexpected initial metrics: %+v", metrics)
	}
	resources.Close()
	resources.Close()
	if _, err := resources.Metrics(); !errors.Is(err, paimon.ErrClosed) {
		t.Fatalf("expected ErrClosed after Close, got %v", err)
	}
}

func TestReadBuilderWithResourceBudget(t *testing.T) {
	source := filepath.Join("testdata", "map_blob_table")
	warehouse := t.TempDir()
	if err := copyDirectory(source, filepath.Join(warehouse, "default.db", "map_blob_table")); err != nil {
		t.Fatal(err)
	}
	table := openTableAt(t, warehouse, "map_blob_table")
	resources, err := paimon.NewResourceContext(0)
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Close()
	builder, err := table.NewReadBuilderWithOptions(map[string]string{"blob-as-descriptor": "true"})
	if err != nil {
		t.Fatal(err)
	}
	defer builder.Close()
	if err := builder.WithResources(resources); err != nil {
		t.Fatal(err)
	}
	resources.Close()
	if err := builder.WithResources(resources); !errors.Is(err, paimon.ErrClosed) {
		t.Fatalf("expected ErrClosed for a closed resource context, got %v", err)
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
	if len(plan.Splits()) == 0 {
		t.Fatal("expected a nonempty read plan")
	}
	read, err := builder.NewRead()
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	reader, err := read.NewRecordBatchReader(plan.Splits())
	if err != nil {
		requireResourceExhausted(t, err)
		return
	}
	defer reader.Close()
	record, err := reader.NextRecord()
	if record != nil {
		record.Release()
	}
	requireResourceExhausted(t, err)
}

func TestWriteBuildersShareResourceBudget(t *testing.T) {
	table := openCopiedTestTable(t)
	resources, err := paimon.NewResourceContext(1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Close()
	builders := make([]*paimon.WriteBuilder, 2)
	writers := make([]*paimon.TableWrite, 2)
	for i := range builders {
		builders[i], err = table.NewWriteBuilder()
		if err != nil {
			t.Fatal(err)
		}
		defer builders[i].Close()
		if err := builders[i].WithResources(resources); err != nil {
			t.Fatal(err)
		}
		writers[i], err = builders[i].NewWrite()
		if err != nil {
			t.Fatal(err)
		}
		defer writers[i].Close()
	}

	value := strings.Repeat("x", 600_000)
	first := makeRecord(t, []row{{1, value}})
	err = writers[0].WriteArrowBatch(first)
	first.Release()
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := resources.Metrics()
	if err != nil {
		t.Fatal(err)
	}
	if metrics.ReservedMemoryBytes == 0 || metrics.ReservedMemoryBytes > 1_000_000 {
		t.Fatalf("unexpected reserved bytes: %+v", metrics)
	}
	second := makeRecord(t, []row{{2, value}})
	err = writers[1].WriteArrowBatch(second)
	second.Release()
	requireResourceExhausted(t, err)
	writers[0].Close()
	metrics, err = resources.Metrics()
	if err != nil {
		t.Fatal(err)
	}
	if metrics.ReservedMemoryBytes != 0 || metrics.PeakReservedMemoryBytes == 0 {
		t.Fatalf("unexpected metrics after releasing writers: %+v", metrics)
	}
}

func TestPostponeWriteBuilderWithResourceBudget(t *testing.T) {
	warehouse := testWarehouse()
	if _, err := os.Stat(filepath.Join(warehouse, "default.db", "postpone_fixed_bucket_pk_table")); os.IsNotExist(err) {
		t.Skip("postpone fixed-bucket test table is unavailable")
	}
	table := openCopiedTable(t, "postpone_fixed_bucket_pk_table")
	resources, err := paimon.NewResourceContext(0)
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Close()
	builder, err := table.NewPostponeFixedBucketWriteBuilder()
	if err != nil {
		t.Fatal(err)
	}
	defer builder.Close()
	if err := builder.WithResources(resources); err != nil {
		t.Fatal(err)
	}
	resources.Close()
	plan := makePartitionedBucketPlan(t, []string{"2026-08-14"}, 1)
	err = builder.WithBucketPlan(plan)
	plan.Release()
	if err != nil {
		t.Fatal(err)
	}
	write, err := builder.NewWrite()
	if err != nil {
		t.Fatal(err)
	}
	defer write.Close()
	record := makePartitionedRecord(t, partitionedRow{4, "dave", "2026-08-14"})
	err = write.WriteArrowBatch(record)
	record.Release()
	if err == nil {
		messages, prepareErr := write.PrepareCommit()
		if messages != nil {
			messages.Close()
		}
		err = prepareErr
	}
	requireResourceExhausted(t, err)
}
