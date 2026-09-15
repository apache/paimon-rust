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
	"context"
	"errors"
	"io"
	"runtime"
	"sort"
	"sync"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/cdata"
	"github.com/jupiterrider/ffi"
)

// TableRead reads data from splits produced by a TableScan.
type TableRead struct {
	ctx       context.Context
	lib       *libRef
	inner     *paimonTableRead
	closeOnce sync.Once
}

// Close releases the table read resources. Safe to call multiple times.
func (tr *TableRead) Close() {
	tr.closeOnce.Do(func() {
		ffiTableReadFree.symbol(tr.ctx)(tr.inner)
		tr.inner = nil
		tr.lib.release()
	})
}

// NewRecordBatchReader creates a RecordBatchReader that iterates over Arrow
// record batches for the given data splits. The splits can be non-contiguous
// and in any order. All splits must originate from the same Plan.
func (tr *TableRead) NewRecordBatchReader(splits []DataSplit) (*RecordBatchReader, error) {
	if tr.inner == nil {
		return nil, ErrClosed
	}
	if len(splits) == 0 {
		return nil, errors.New("paimon: splits must not be empty")
	}

	// All splits must share the same plan handle.
	handle := splits[0].set.handle
	indices := make([]int, len(splits))
	for i, s := range splits {
		if s.set.handle != handle {
			return nil, errors.New("paimon: all splits must originate from the same Plan")
		}
		indices[i] = s.index
	}
	sort.Ints(indices)

	// Group sorted indices into contiguous ranges.
	type spanRange struct{ offset, length int }
	ranges := []spanRange{{offset: indices[0], length: 1}}
	for _, idx := range indices[1:] {
		last := &ranges[len(ranges)-1]
		if idx == last.offset+last.length {
			last.length++
		} else {
			ranges = append(ranges, spanRange{offset: idx, length: 1})
		}
	}

	// Create one C reader per contiguous range.
	createFn := ffiTableReadToArrow.symbol(tr.ctx)
	readers := make([]*paimonRecordBatchReader, 0, len(ranges))
	for _, r := range ranges {
		inner, err := createFn(tr.inner, handle.inner, uintptr(r.offset), uintptr(r.length))
		if err != nil {
			// Free already-created readers on error.
			freeFn := ffiRecordBatchReaderFree.symbol(tr.ctx)
			for _, rd := range readers {
				freeFn(rd)
				tr.lib.release()
			}
			return nil, err
		}
		tr.lib.acquire()
		readers = append(readers, inner)
	}

	return &RecordBatchReader{ctx: tr.ctx, lib: tr.lib, readers: readers}, nil
}

// RecordBatchReader iterates over Arrow record batches one at a time via
// the Arrow C Data Interface (zero-copy). Call NextRecord to advance and
// Close when done.
type RecordBatchReader struct {
	ctx       context.Context
	lib       *libRef
	mu        sync.Mutex
	readers   []*paimonRecordBatchReader
	current   int
	closeOnce sync.Once
}

// NextRecord returns the next Arrow record, or io.EOF when iteration is
// complete. The underlying C batch is imported via the Arrow C Data Interface
// and released automatically — the caller only needs to call Release on the
// returned arrow.Record when done. Calls on one reader are serialized; use
// separate readers for parallel reads.
func (r *RecordBatchReader) NextRecord() (arrow.Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.readers == nil {
		return nil, ErrClosed
	}
	batch, err := r.next()
	if err != nil {
		return nil, err
	}
	record, err := cdata.ImportCRecordBatch(
		(*cdata.CArrowArray)(batch.array),
		(*cdata.CArrowSchema)(batch.schema),
	)
	batch.release()
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (r *RecordBatchReader) next() (*arrowBatch, error) {
	nextFn := ffiRecordBatchReaderNext.symbol(r.ctx)
	for r.current < len(r.readers) {
		array, schema, err := nextFn(r.readers[r.current])
		if err != nil {
			return nil, err
		}
		if array == nil && schema == nil {
			r.current++
			continue
		}
		r.lib.acquire()
		ab := &arrowBatch{ctx: r.ctx, lib: r.lib, array: array, schema: schema}
		runtime.SetFinalizer(ab, (*arrowBatch).release)
		return ab, nil
	}
	return nil, io.EOF
}

// Close releases the underlying C record batch readers. Safe to call multiple times
// and concurrently with NextRecord.
func (r *RecordBatchReader) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.closeOnce.Do(func() {
		freeFn := ffiRecordBatchReaderFree.symbol(r.ctx)
		for _, rd := range r.readers {
			freeFn(rd)
			r.lib.release()
		}
		r.readers = nil
	})
}

var ffiTableReadFree = newFFI(ffiOpts{
	sym:    "paimon_table_read_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(read *paimonTableRead) {
	return func(read *paimonTableRead) {
		ffiCall(
			nil,
			unsafe.Pointer(&read),
		)
	}
})

var ffiTableReadToArrow = newFFI(ffiOpts{
	sym:    "paimon_table_read_to_arrow",
	rType:  &typeResultRecordBatchReader,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(read *paimonTableRead, plan *paimonPlan, offset uintptr, length uintptr) (*paimonRecordBatchReader, error) {
	return func(read *paimonTableRead, plan *paimonPlan, offset uintptr, length uintptr) (*paimonRecordBatchReader, error) {
		var result resultRecordBatchReader
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&read),
			unsafe.Pointer(&plan),
			unsafe.Pointer(&offset),
			unsafe.Pointer(&length),
		)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.reader, nil
	}
})

var ffiRecordBatchReaderNext = newFFI(ffiOpts{
	sym:    "paimon_record_batch_reader_next",
	rType:  &typeResultNextBatch,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(reader *paimonRecordBatchReader) (unsafe.Pointer, unsafe.Pointer, error) {
	return func(reader *paimonRecordBatchReader) (unsafe.Pointer, unsafe.Pointer, error) {
		var result resultNextBatch
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&reader),
		)
		if result.error != nil {
			return nil, nil, parseError(ctx, result.error)
		}
		return result.array, result.schema, nil
	}
})

var ffiRecordBatchReaderFree = newFFI(ffiOpts{
	sym:    "paimon_record_batch_reader_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(reader *paimonRecordBatchReader) {
	return func(reader *paimonRecordBatchReader) {
		ffiCall(
			nil,
			unsafe.Pointer(&reader),
		)
	}
})

var ffiArrowBatchFree = newFFI(ffiOpts{
	sym:    "paimon_arrow_batch_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&typeArrowBatch},
}, func(_ context.Context, ffiCall ffiCall) func(array unsafe.Pointer, schema unsafe.Pointer) {
	return func(array unsafe.Pointer, schema unsafe.Pointer) {
		batch := struct {
			array  unsafe.Pointer
			schema unsafe.Pointer
		}{array: array, schema: schema}
		ffiCall(
			nil,
			unsafe.Pointer(&batch),
		)
	}
})
