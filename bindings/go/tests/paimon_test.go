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
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	paimon "github.com/apache/paimon-rust/bindings/go"
)

type row struct {
	id   int32
	name string
}

func testWarehouse() string {
	warehouse := os.Getenv("PAIMON_TEST_WAREHOUSE")
	if warehouse == "" {
		return "/tmp/paimon-warehouse"
	}
	return warehouse
}

func copyDirectory(source, target string) error {
	info, err := os.Stat(source)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(target, info.Mode()); err != nil {
		return err
	}

	entries, err := os.ReadDir(source)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		sourcePath := filepath.Join(source, entry.Name())
		targetPath := filepath.Join(target, entry.Name())
		if entry.IsDir() {
			if err := copyDirectory(sourcePath, targetPath); err != nil {
				return err
			}
			continue
		}

		entryInfo, err := entry.Info()
		if err != nil {
			return err
		}
		input, err := os.Open(sourcePath)
		if err != nil {
			return err
		}
		output, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, entryInfo.Mode())
		if err != nil {
			input.Close()
			return err
		}
		_, copyErr := io.Copy(output, input)
		inputCloseErr := input.Close()
		outputCloseErr := output.Close()
		if copyErr != nil {
			return copyErr
		}
		if inputCloseErr != nil {
			return inputCloseErr
		}
		if outputCloseErr != nil {
			return outputCloseErr
		}
	}
	return nil
}

func openTableAt(t *testing.T, warehouse, tableName string) *paimon.Table {
	t.Helper()

	catalog, err := paimon.NewCatalog(map[string]string{
		"warehouse": warehouse,
	})
	if err != nil {
		t.Fatalf("Failed to create catalog: %v", err)
	}
	t.Cleanup(func() { catalog.Close() })

	table, err := catalog.GetTable(paimon.NewIdentifier("default", tableName))
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}
	t.Cleanup(func() { table.Close() })
	return table
}

func openCopiedTestTable(t *testing.T) *paimon.Table {
	return openCopiedTable(t, "simple_pk_table")
}

func openCopiedTable(t *testing.T, tableName string) *paimon.Table {
	t.Helper()

	warehouse := testWarehouse()
	source := filepath.Join(warehouse, "default.db", tableName)
	if _, err := os.Stat(source); os.IsNotExist(err) {
		t.Skipf("Skipping: table %s does not exist (run 'make docker-up' first)", source)
	}

	targetWarehouse := t.TempDir()
	target := filepath.Join(targetWarehouse, "default.db", tableName)
	if err := copyDirectory(source, target); err != nil {
		t.Fatalf("Failed to copy test table: %v", err)
	}
	return openTableAt(t, targetWarehouse, tableName)
}

func makeRecord(t *testing.T, rows []row) arrow.Record {
	t.Helper()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	idBuilder := builder.Field(0).(*array.Int32Builder)
	nameBuilder := builder.Field(1).(*array.StringBuilder)
	for _, value := range rows {
		idBuilder.Append(value.id)
		nameBuilder.Append(value.name)
	}
	return builder.NewRecord()
}

func readTableRows(t *testing.T, table *paimon.Table) []row {
	t.Helper()
	rb, err := table.NewReadBuilder()
	if err != nil {
		t.Fatalf("Failed to create read builder: %v", err)
	}
	defer rb.Close()
	return readRows(t, rb)
}

// readRows scans and reads all (id, name) rows from a ReadBuilder.
func readRows(t *testing.T, rb *paimon.ReadBuilder) []row {
	t.Helper()

	scan, err := rb.NewScan()
	if err != nil {
		t.Fatalf("Failed to create scan: %v", err)
	}
	defer scan.Close()

	plan, err := scan.Plan()
	if err != nil {
		t.Fatalf("Failed to plan: %v", err)
	}
	defer plan.Close()

	splits := plan.Splits()
	if len(splits) == 0 {
		return nil
	}

	read, err := rb.NewRead()
	if err != nil {
		t.Fatalf("Failed to create table read: %v", err)
	}
	defer read.Close()

	reader, err := read.NewRecordBatchReader(splits)
	if err != nil {
		t.Fatalf("Failed to create record batch reader: %v", err)
	}
	defer reader.Close()

	var rows []row
	batchIdx := 0
	for {
		record, err := reader.NextRecord()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Batch %d: failed to read next record: %v", batchIdx, err)
		}

		idIdx := record.Schema().FieldIndices("id")
		nameIdx := record.Schema().FieldIndices("name")
		if len(idIdx) == 0 || len(nameIdx) == 0 {
			record.Release()
			t.Fatalf("Batch %d: missing expected columns (id, name) in schema: %s", batchIdx, record.Schema())
		}

		idCol := record.Column(idIdx[0]).(*array.Int32)
		nameCol := record.Column(nameIdx[0]).(*array.String)

		for j := 0; j < int(record.NumRows()); j++ {
			rows = append(rows, row{
				id:   idCol.Value(j),
				name: string([]byte(nameCol.Value(j))),
			})
		}
		record.Release()
		batchIdx++
	}
	return rows
}

// openTestTable creates a catalog, opens the simple_log_table, and returns
// the table along with a cleanup function. Skips the test if the warehouse
// does not exist.
func openTestTable(t *testing.T) *paimon.Table {
	t.Helper()

	warehouse := testWarehouse()
	if _, err := os.Stat(warehouse); os.IsNotExist(err) {
		t.Skipf("Skipping: warehouse %s does not exist (run 'make docker-up' first)", warehouse)
	}
	return openTableAt(t, warehouse, "simple_log_table")
}

func TestWriteCommitReadRoundTrip(t *testing.T) {
	table := openCopiedTestTable(t)

	builder, err := table.NewWriteBuilder()
	if err != nil {
		t.Fatalf("Failed to create write builder: %v", err)
	}
	defer builder.Close()

	write, err := builder.NewWrite()
	if err != nil {
		t.Fatalf("Failed to create table write: %v", err)
	}
	defer write.Close()

	record := makeRecord(t, []row{{4, "dave"}})
	if err := write.WriteArrowBatch(record); err != nil {
		record.Release()
		t.Fatalf("Failed to write Arrow record batch: %v", err)
	}
	record.Release()

	messages, err := write.PrepareCommit()
	if err != nil {
		t.Fatalf("Failed to prepare commit: %v", err)
	}
	defer messages.Close()

	commit, err := builder.NewCommit()
	if err != nil {
		t.Fatalf("Failed to create table commit: %v", err)
	}
	defer commit.Close()
	if err := commit.Commit(messages); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	rows := readTableRows(t, table)
	sort.Slice(rows, func(i, j int) bool { return rows[i].id < rows[j].id })
	expected := []row{{1, "alice"}, {2, "bob"}, {3, "carol"}, {4, "dave"}}
	if len(rows) != len(expected) {
		t.Fatalf("Expected %d rows, got %d: %v", len(expected), len(rows), rows)
	}
	for i := range expected {
		if rows[i] != expected[i] {
			t.Errorf("Row %d: expected %v, got %v", i, expected[i], rows[i])
		}
	}
}

func TestWriteOverwriteUsesBuilderMode(t *testing.T) {
	table := openCopiedTestTable(t)

	builder, err := table.NewWriteBuilder()
	if err != nil {
		t.Fatalf("Failed to create write builder: %v", err)
	}
	defer builder.Close()
	if err := builder.WithOverwrite(); err != nil {
		t.Fatalf("Failed to enable overwrite: %v", err)
	}

	write, err := builder.NewWrite()
	if err != nil {
		t.Fatalf("Failed to create table write: %v", err)
	}
	defer write.Close()
	record := makeRecord(t, []row{{4, "dave"}})
	if err := write.WriteArrowBatch(record); err != nil {
		record.Release()
		t.Fatalf("Failed to write Arrow record batch: %v", err)
	}
	record.Release()

	messages, err := write.PrepareCommit()
	if err != nil {
		t.Fatalf("Failed to prepare commit: %v", err)
	}
	defer messages.Close()
	commit, err := builder.NewCommit()
	if err != nil {
		t.Fatalf("Failed to create table commit: %v", err)
	}
	defer commit.Close()
	if err := commit.Commit(messages); err != nil {
		t.Fatalf("Failed to overwrite: %v", err)
	}

	rows := readTableRows(t, table)
	expected := []row{{4, "dave"}}
	if len(rows) != len(expected) || rows[0] != expected[0] {
		t.Fatalf("Expected %v after overwrite, got %v", expected, rows)
	}
}

func TestOverwriteRetrySameIdentifierIsIdempotent(t *testing.T) {
	warehouse := testWarehouse()
	source := filepath.Join(warehouse, "default.db", "simple_pk_table")
	if _, err := os.Stat(source); os.IsNotExist(err) {
		t.Skipf("Skipping: table %s does not exist (run 'make docker-up' first)", source)
	}
	targetWarehouse := t.TempDir()
	target := filepath.Join(targetWarehouse, "default.db", "simple_pk_table")
	if err := copyDirectory(source, target); err != nil {
		t.Fatalf("Failed to copy test table: %v", err)
	}
	table := openTableAt(t, targetWarehouse, "simple_pk_table")

	countSnapshots := func() int {
		entries, err := os.ReadDir(filepath.Join(target, "snapshot"))
		if err != nil {
			t.Fatalf("Failed to list snapshots: %v", err)
		}
		count := 0
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), "snapshot-") {
				count++
			}
		}
		return count
	}

	const commitUser = "go-binding-overwrite-retry"
	overwriteAndPrepare := func() (*paimon.WriteBuilder, *paimon.CommitMessages) {
		builder, err := table.NewWriteBuilderWithCommitUser(commitUser)
		if err != nil {
			t.Fatalf("Failed to create write builder: %v", err)
		}
		t.Cleanup(builder.Close)
		if err := builder.WithOverwrite(); err != nil {
			t.Fatalf("Failed to enable overwrite: %v", err)
		}
		write, err := builder.NewWrite()
		if err != nil {
			t.Fatalf("Failed to create table write: %v", err)
		}
		defer write.Close()
		record := makeRecord(t, []row{{4, "dave"}})
		if err := write.WriteArrowBatch(record); err != nil {
			record.Release()
			t.Fatalf("Failed to write Arrow record batch: %v", err)
		}
		record.Release()
		messages, err := write.PrepareCommit()
		if err != nil {
			t.Fatalf("Failed to prepare commit: %v", err)
		}
		t.Cleanup(messages.Close)
		return builder, messages
	}

	builder, messages := overwriteAndPrepare()
	commit, err := builder.NewCommit()
	if err != nil {
		t.Fatalf("Failed to create table commit: %v", err)
	}
	defer commit.Close()
	if err := commit.CommitWithIdentifier(messages, 7); err != nil {
		t.Fatalf("Failed to overwrite with identifier: %v", err)
	}

	appendBuilder, err := table.NewWriteBuilder()
	if err != nil {
		t.Fatalf("Failed to create append write builder: %v", err)
	}
	defer appendBuilder.Close()
	appendWrite, err := appendBuilder.NewWrite()
	if err != nil {
		t.Fatalf("Failed to create append table write: %v", err)
	}
	defer appendWrite.Close()
	record := makeRecord(t, []row{{5, "eve"}})
	if err := appendWrite.WriteArrowBatch(record); err != nil {
		record.Release()
		t.Fatalf("Failed to write append batch: %v", err)
	}
	record.Release()
	appendMessages, err := appendWrite.PrepareCommit()
	if err != nil {
		t.Fatalf("Failed to prepare append commit: %v", err)
	}
	defer appendMessages.Close()
	appendCommit, err := appendBuilder.NewCommit()
	if err != nil {
		t.Fatalf("Failed to create append table commit: %v", err)
	}
	defer appendCommit.Close()
	if err := appendCommit.Commit(appendMessages); err != nil {
		t.Fatalf("Failed to append: %v", err)
	}
	snapshotsBeforeRetry := countSnapshots()

	retryBuilder, err := table.NewWriteBuilderWithCommitUser(commitUser)
	if err != nil {
		t.Fatalf("Failed to create retry write builder: %v", err)
	}
	defer retryBuilder.Close()
	if err := retryBuilder.WithOverwrite(); err != nil {
		t.Fatalf("Failed to enable overwrite on retry builder: %v", err)
	}
	retryCommit, err := retryBuilder.NewCommit()
	if err != nil {
		t.Fatalf("Failed to create retry table commit: %v", err)
	}
	defer retryCommit.Close()
	if err := retryCommit.FilterAndCommitWithIdentifier(messages, 7); err != nil {
		t.Fatalf("Failed to retry overwrite idempotently: %v", err)
	}

	if got := countSnapshots(); got != snapshotsBeforeRetry {
		t.Fatalf("Retry added snapshots: %d != %d", got, snapshotsBeforeRetry)
	}
	rows := readTableRows(t, table)
	sort.Slice(rows, func(i, j int) bool { return rows[i].id < rows[j].id })
	expected := []row{{4, "dave"}, {5, "eve"}}
	if len(rows) != len(expected) {
		t.Fatalf("Expected %v after retry, got %v", expected, rows)
	}
	for i := range expected {
		if rows[i] != expected[i] {
			t.Errorf("Row %d: expected %v, got %v", i, expected[i], rows[i])
		}
	}
}

func TestAppendOnlyWriteMergeAndIdempotentCommit(t *testing.T) {
	table := openCopiedTable(t, "simple_log_table")
	const commitUser = "go-binding-multiple-writers"

	builder1, err := table.NewWriteBuilderWithCommitUser(commitUser)
	if err != nil {
		t.Fatalf("Failed to create first write builder: %v", err)
	}
	defer builder1.Close()
	builder2, err := table.NewWriteBuilderWithCommitUser(commitUser)
	if err != nil {
		t.Fatalf("Failed to create second write builder: %v", err)
	}
	defer builder2.Close()

	writeAndPrepare := func(builder *paimon.WriteBuilder, value row) *paimon.CommitMessages {
		write, err := builder.NewWrite()
		if err != nil {
			t.Fatalf("Failed to create table write: %v", err)
		}
		defer write.Close()
		record := makeRecord(t, []row{value})
		if err := write.WriteArrowBatch(record); err != nil {
			record.Release()
			t.Fatalf("Failed to write Arrow record batch: %v", err)
		}
		record.Release()
		messages, err := write.PrepareCommit()
		if err != nil {
			t.Fatalf("Failed to prepare commit: %v", err)
		}
		return messages
	}

	messages1 := writeAndPrepare(builder1, row{4, "dave"})
	defer messages1.Close()
	messages2 := writeAndPrepare(builder2, row{5, "eve"})
	defer messages2.Close()
	if err := messages1.Merge(messages2); err != nil {
		t.Fatalf("Failed to merge commit messages: %v", err)
	}

	commit, err := builder1.NewCommit()
	if err != nil {
		t.Fatalf("Failed to create table commit: %v", err)
	}
	defer commit.Close()
	if err := commit.CommitWithIdentifier(messages1, 7); err != nil {
		t.Fatalf("Failed to commit with identifier: %v", err)
	}

	retryBuilder, err := table.NewWriteBuilderWithCommitUser(commitUser)
	if err != nil {
		t.Fatalf("Failed to create retry write builder: %v", err)
	}
	defer retryBuilder.Close()
	retryCommit, err := retryBuilder.NewCommit()
	if err != nil {
		t.Fatalf("Failed to create retry table commit: %v", err)
	}
	defer retryCommit.Close()
	if err := retryCommit.FilterAndCommitWithIdentifier(messages1, 7); err != nil {
		t.Fatalf("Failed to retry commit idempotently: %v", err)
	}

	rows := readTableRows(t, table)
	sort.Slice(rows, func(i, j int) bool { return rows[i].id < rows[j].id })
	expected := []row{{1, "alice"}, {2, "bob"}, {3, "carol"}, {4, "dave"}, {5, "eve"}}
	if len(rows) != len(expected) {
		t.Fatalf("Expected %d rows, got %d: %v", len(expected), len(rows), rows)
	}
	for i := range expected {
		if rows[i] != expected[i] {
			t.Errorf("Row %d: expected %v, got %v", i, expected[i], rows[i])
		}
	}
}

// TestReadLogTable reads the test table and verifies the data matches expected values.
//
// The table was populated by Docker provisioning with:
//
//	(1, 'alice'), (2, 'bob'), (3, 'carol')
func TestReadLogTable(t *testing.T) {
	table := openTestTable(t)

	rb, err := table.NewReadBuilder()
	if err != nil {
		t.Fatalf("Failed to create read builder: %v", err)
	}
	defer rb.Close()

	rows := readRows(t, rb)
	if len(rows) == 0 {
		t.Fatal("Expected at least one row, got 0")
	}

	sort.Slice(rows, func(i, j int) bool { return rows[i].id < rows[j].id })

	expected := []row{{1, "alice"}, {2, "bob"}, {3, "carol"}}
	if len(rows) != len(expected) {
		t.Fatalf("Expected %d rows, got %d: %v", len(expected), len(rows), rows)
	}
	for i, exp := range expected {
		if rows[i] != exp {
			t.Errorf("Row %d: expected %v, got %v", i, exp, rows[i])
		}
	}
}

// TestReadWithFilter exercises filter push-down through several sub-tests.
func TestReadWithFilter(t *testing.T) {
	table := openTestTable(t)

	t.Run("EqualById", func(t *testing.T) {
		rb, err := table.NewReadBuilder()
		if err != nil {
			t.Fatalf("Failed to create read builder: %v", err)
		}
		defer rb.Close()

		// id = 1
		pb := table.PredicateBuilder()
		pred, err := pb.Eq("id", 1)
		if err != nil {
			t.Fatalf("Failed to create predicate: %v", err)
		}
		if err := rb.WithFilter(pred); err != nil {
			t.Fatalf("Failed to set filter: %v", err)
		}

		rows := readRows(t, rb)
		expected := []row{{1, "alice"}}
		if len(rows) != len(expected) {
			t.Fatalf("Expected %d rows, got %d: %v", len(expected), len(rows), rows)
		}
		if rows[0] != expected[0] {
			t.Errorf("Expected %v, got %v", expected[0], rows[0])
		}
	})

	t.Run("EmptyStringEqual", func(t *testing.T) {
		rb, err := table.NewReadBuilder()
		if err != nil {
			t.Fatalf("Failed to create read builder: %v", err)
		}
		defer rb.Close()

		pb := table.PredicateBuilder()
		pred, err := pb.Eq("name", "")
		if err != nil {
			t.Fatalf("Eq with empty string failed: %v", err)
		}
		if err := rb.WithFilter(pred); err != nil {
			t.Fatalf("WithFilter failed: %v", err)
		}

		rows := readRows(t, rb)
		if len(rows) != 0 {
			t.Fatalf("Expected 0 rows for empty string filter, got %d: %v", len(rows), rows)
		}
	})
}

// TestReadWithProjection reads only the "id" column via WithProjection and
// verifies that only the projected column is returned with correct values.
func TestReadWithProjection(t *testing.T) {
	table := openTestTable(t)

	rb, err := table.NewReadBuilder()
	if err != nil {
		t.Fatalf("Failed to create read builder: %v", err)
	}
	defer rb.Close()

	if err := rb.WithProjection([]string{"id"}); err != nil {
		t.Fatalf("Failed to set projection: %v", err)
	}

	scan, err := rb.NewScan()
	if err != nil {
		t.Fatalf("Failed to create scan: %v", err)
	}
	defer scan.Close()

	plan, err := scan.Plan()
	if err != nil {
		t.Fatalf("Failed to plan: %v", err)
	}
	defer plan.Close()

	splits := plan.Splits()
	if len(splits) == 0 {
		t.Fatal("Expected at least one split")
	}

	read, err := rb.NewRead()
	if err != nil {
		t.Fatalf("Failed to create table read: %v", err)
	}
	defer read.Close()

	reader, err := read.NewRecordBatchReader(splits)
	if err != nil {
		t.Fatalf("Failed to create record batch reader: %v", err)
	}
	defer reader.Close()

	var ids []int32
	batchIdx := 0
	for {
		record, err := reader.NextRecord()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Batch %d: failed to read next record: %v", batchIdx, err)
		}

		schema := record.Schema()
		if schema.NumFields() != 1 {
			record.Release()
			t.Fatalf("Batch %d: expected 1 field, got %d: %s", batchIdx, schema.NumFields(), schema)
		}
		if schema.Field(0).Name != "id" {
			record.Release()
			t.Fatalf("Batch %d: expected field 'id', got '%s'", batchIdx, schema.Field(0).Name)
		}

		idCol := record.Column(0).(*array.Int32)
		for j := 0; j < int(record.NumRows()); j++ {
			ids = append(ids, idCol.Value(j))
		}
		record.Release()
		batchIdx++
	}

	if len(ids) == 0 {
		t.Fatal("Expected at least one row, got 0")
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	expected := []int32{1, 2, 3}
	if len(ids) != len(expected) {
		t.Fatalf("Expected %d rows, got %d: %v", len(expected), len(ids), ids)
	}
	for i, exp := range expected {
		if ids[i] != exp {
			t.Errorf("Row %d: expected id=%d, got id=%d", i, exp, ids[i])
		}
	}
}
