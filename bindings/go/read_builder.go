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
	"runtime"
	"sync"
	"unsafe"

	"github.com/jupiterrider/ffi"
)

// ReadBuilder creates TableScan and TableRead instances.
type ReadBuilder struct {
	ctx       context.Context
	lib       *libRef
	inner     *paimonReadBuilder
	closeOnce sync.Once
}

// Close releases the read builder resources. Safe to call multiple times.
func (rb *ReadBuilder) Close() {
	rb.closeOnce.Do(func() {
		ffiReadBuilderFree.symbol(rb.ctx)(rb.inner)
		rb.inner = nil
		rb.lib.release()
	})
}

// WithProjection sets column projection by name. Output order follows the
// caller-specified order. A name that matches no schema column under any case
// sensitivity is rejected immediately; case-dependent errors and duplicate names
// cause NewRead() to fail. An empty list is a valid zero-column projection.
func (rb *ReadBuilder) WithProjection(columns []string) error {
	if rb.inner == nil {
		return ErrClosed
	}
	projFn := ffiReadBuilderWithProjection.symbol(rb.ctx)
	return projFn(rb.inner, columns)
}

// WithCaseSensitive sets whether the names given to WithProjection must match
// the schema exactly. The default is true. With false, names are matched by
// ASCII case folding, and a name that folds onto two different schema columns is
// rejected as ambiguous. Either way the returned records carry the schema's own
// spelling, not the requested one.
//
// This does not affect predicates. A predicate resolves its column when it is
// built, so its case sensitivity comes from the builder that produced it — see
// PredicateBuilder.WithCaseSensitive — and is unaffected by this setting. Call
// order relative to WithProjection does not matter: projection names are resolved
// in NewRead.
func (rb *ReadBuilder) WithCaseSensitive(caseSensitive bool) error {
	if rb.inner == nil {
		return ErrClosed
	}
	return ffiReadBuilderWithCaseSensitive.symbol(rb.ctx)(rb.inner, caseSensitive)
}

// WithFilter sets a filter predicate for scan planning and read-side pruning.
//
// The predicate is used in two phases:
//   - Scan planning: prunes partitions, buckets, and data files based on
//     file-level statistics (min/max). This is conservative — files whose
//     statistics are inconclusive are kept.
//   - Read-side: applies row-level filtering via Parquet native row filters
//     for supported leaf predicates (Eq, NotEq, Lt, Le, Gt, Ge, IsNull,
//     IsNotNull, In, NotIn).
//
// Row-level filtering is exact for most common types (Bool, Int, Long, Float,
// Double, String, Date, Decimal, Binary). However, the following cases are NOT
// filtered at the row level and may return non-matching rows:
//   - Compound predicates (And/Or/Not) — not yet implemented for row-level filtering.
//   - Time, Timestamp, and LocalZonedTimestamp columns (not yet implemented).
//   - Schema-evolution: the predicate column does not exist in older data files.
//   - Data-evolution mode (data-evolution.enabled = true).
//
// In these cases callers should apply residual filtering on the returned records.
//
// The predicate is consumed (ownership transferred to the read builder);
// the caller must NOT close it after this call.
// Passing nil is a no-op.
func (rb *ReadBuilder) WithFilter(p *Predicate) error {
	if rb.inner == nil {
		return ErrClosed
	}
	if p == nil {
		return nil
	}
	if p.inner == nil {
		return errConsumedPredicate
	}
	filterFn := ffiReadBuilderWithFilter.symbol(rb.ctx)
	err := filterFn(rb.inner, p.inner)
	// Ownership transferred; prevent double-free.
	p.inner = nil
	p.lib.release()
	return err
}

// NewScan creates a TableScan for planning which data files to read.
func (rb *ReadBuilder) NewScan() (*TableScan, error) {
	if rb.inner == nil {
		return nil, ErrClosed
	}
	createFn := ffiReadBuilderNewScan.symbol(rb.ctx)
	inner, err := createFn(rb.inner)
	if err != nil {
		return nil, err
	}
	rb.lib.acquire()
	return &TableScan{ctx: rb.ctx, lib: rb.lib, inner: inner}, nil
}

// NewRead creates a TableRead for reading data from splits.
func (rb *ReadBuilder) NewRead() (*TableRead, error) {
	if rb.inner == nil {
		return nil, ErrClosed
	}
	createFn := ffiReadBuilderNewRead.symbol(rb.ctx)
	inner, err := createFn(rb.inner)
	if err != nil {
		return nil, err
	}
	rb.lib.acquire()
	return &TableRead{ctx: rb.ctx, lib: rb.lib, inner: inner}, nil
}

var ffiReadBuilderFree = newFFI(ffiOpts{
	sym:    "paimon_read_builder_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(rb *paimonReadBuilder) {
	return func(rb *paimonReadBuilder) {
		ffiCall(
			nil,
			unsafe.Pointer(&rb),
		)
	}
})

var ffiReadBuilderWithProjection = newFFI(ffiOpts{
	sym:    "paimon_read_builder_with_projection",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(rb *paimonReadBuilder, columns []string) error {
	return func(rb *paimonReadBuilder, columns []string) error {
		var colPtrs []*byte
		var cStrings [][]byte

		// Convert Go strings to null-terminated C strings
		for _, col := range columns {
			cStr := append([]byte(col), 0)
			cStrings = append(cStrings, cStr)
			colPtrs = append(colPtrs, &cStr[0])
		}
		// Null-terminate the array
		colPtrs = append(colPtrs, nil)

		var colsPtr unsafe.Pointer
		if len(colPtrs) > 0 {
			colsPtr = unsafe.Pointer(&colPtrs[0])
		}

		var errPtr *paimonError
		ffiCall(
			unsafe.Pointer(&errPtr),
			unsafe.Pointer(&rb),
			unsafe.Pointer(&colsPtr),
		)
		// Ensure Go-managed buffers stay alive for the full native call.
		runtime.KeepAlive(cStrings)
		runtime.KeepAlive(colPtrs)
		if errPtr != nil {
			return parseError(ctx, errPtr)
		}
		return nil
	}
})

// The trailing `bool` is passed as a 1-byte integer written through boolByte; see
// that function for why.
var ffiReadBuilderWithCaseSensitive = newFFI(ffiOpts{
	sym:    "paimon_read_builder_with_case_sensitive",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypeUint8},
}, func(ctx context.Context, ffiCall ffiCall) func(rb *paimonReadBuilder, caseSensitive bool) error {
	return func(rb *paimonReadBuilder, caseSensitive bool) error {
		flag := boolByte(caseSensitive)
		var errPtr *paimonError
		ffiCall(
			unsafe.Pointer(&errPtr),
			unsafe.Pointer(&rb),
			unsafe.Pointer(&flag),
		)
		if errPtr != nil {
			return parseError(ctx, errPtr)
		}
		return nil
	}
})

var ffiReadBuilderWithFilter = newFFI(ffiOpts{
	sym:    "paimon_read_builder_with_filter",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(rb *paimonReadBuilder, p *paimonPredicate) error {
	return func(rb *paimonReadBuilder, p *paimonPredicate) error {
		var errPtr *paimonError
		ffiCall(
			unsafe.Pointer(&errPtr),
			unsafe.Pointer(&rb),
			unsafe.Pointer(&p),
		)
		if errPtr != nil {
			return parseError(ctx, errPtr)
		}
		return nil
	}
})

var ffiReadBuilderNewScan = newFFI(ffiOpts{
	sym:    "paimon_read_builder_new_scan",
	rType:  &typeResultTableScan,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(rb *paimonReadBuilder) (*paimonTableScan, error) {
	return func(rb *paimonReadBuilder) (*paimonTableScan, error) {
		var result resultTableScan
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&rb),
		)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.scan, nil
	}
})

var ffiReadBuilderNewRead = newFFI(ffiOpts{
	sym:    "paimon_read_builder_new_read",
	rType:  &typeResultNewRead,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(rb *paimonReadBuilder) (*paimonTableRead, error) {
	return func(rb *paimonReadBuilder) (*paimonTableRead, error) {
		var result resultNewRead
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&rb),
		)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.read, nil
	}
})
