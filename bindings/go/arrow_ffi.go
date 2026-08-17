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

package paimon

import (
	"errors"
	"fmt"
	"runtime"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
	"github.com/apache/arrow-go/v18/arrow/memory/mallocator"
)

// cloneRecordToCMemory copies Arrow buffers into C-owned memory so native
// writers can retain record batches until PrepareCommit.
func cloneRecordToCMemory(record arrow.Record) (arrow.Record, error) {
	allocator := mallocator.NewMallocator()
	columns := make([]arrow.Array, record.NumCols())
	for index := range columns {
		column, err := array.Concatenate([]arrow.Array{record.Column(index)}, allocator)
		if err != nil {
			for _, allocated := range columns[:index] {
				allocated.Release()
			}
			return nil, fmt.Errorf("paimon: failed to copy Arrow column %d to C memory: %w", index, err)
		}
		columns[index] = column
	}
	owned := array.NewRecord(record.Schema(), columns, record.NumRows())
	for _, column := range columns {
		column.Release()
	}
	return owned, nil
}

func withOwnedArrowRecord(
	record arrow.Record,
	nilMessage string,
	operation func(unsafe.Pointer, unsafe.Pointer) error,
) error {
	if record == nil {
		return errors.New(nilMessage)
	}
	owned, err := cloneRecordToCMemory(record)
	if err != nil {
		return err
	}
	defer owned.Release()

	var array cdata.CArrowArray
	var schema cdata.CArrowSchema
	cdata.ExportArrowRecordBatch(owned, &array, &schema)
	// The native side imports via from_raw, marking these released
	// (release = NULL); the defers only fire if an error skips the import.
	defer cdata.ReleaseCArrowArray(&array)
	defer cdata.ReleaseCArrowSchema(&schema)

	err = operation(unsafe.Pointer(&array), unsafe.Pointer(&schema))
	runtime.KeepAlive(owned)
	return err
}
