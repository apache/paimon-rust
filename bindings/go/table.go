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

// Table represents a paimon table.
type Table struct {
	ctx       context.Context
	lib       *libRef
	inner     *paimonTable
	closeOnce sync.Once
}

// Close releases the table resources. Safe to call multiple times.
func (t *Table) Close() {
	t.closeOnce.Do(func() {
		ffiTableFree.symbol(t.ctx)(t.inner)
		t.inner = nil
		t.lib.release()
	})
}

// PredicateBuilder returns a builder for creating filter predicates on this table.
func (t *Table) PredicateBuilder() *PredicateBuilder {
	return &PredicateBuilder{table: t}
}

// NewReadBuilder creates a ReadBuilder for this table.
func (t *Table) NewReadBuilder() (*ReadBuilder, error) {
	if t.inner == nil {
		return nil, ErrClosed
	}
	inner, err := ffiTableNewReadBuilder.symbol(t.ctx)(t.inner)
	if err != nil {
		return nil, err
	}
	t.lib.acquire()
	return &ReadBuilder{ctx: t.ctx, lib: t.lib, inner: inner}, nil
}

// NewReadBuilderWithOptions creates a ReadBuilder with per-read options.
func (t *Table) NewReadBuilderWithOptions(options map[string]string) (*ReadBuilder, error) {
	if t.inner == nil {
		return nil, ErrClosed
	}
	inner, err := ffiTableNewReadBuilderWithOptions.symbol(t.ctx)(t.inner, options)
	if err != nil {
		return nil, err
	}
	t.lib.acquire()
	return &ReadBuilder{ctx: t.ctx, lib: t.lib, inner: inner}, nil
}

var ffiTableFree = newFFI(ffiOpts{
	sym:    "paimon_table_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(table *paimonTable) {
	return func(table *paimonTable) {
		ffiCall(
			nil,
			unsafe.Pointer(&table),
		)
	}
})

var ffiTableNewReadBuilder = newFFI(ffiOpts{
	sym:    "paimon_table_new_read_builder",
	rType:  &typeResultReadBuilder,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(table *paimonTable) (*paimonReadBuilder, error) {
	return func(table *paimonTable) (*paimonReadBuilder, error) {
		var result resultReadBuilder
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&table),
		)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.readBuilder, nil
	}
})

var ffiTableNewReadBuilderWithOptions = newFFI(ffiOpts{
	sym:   "paimon_table_new_read_builder_with_options",
	rType: &typeResultReadBuilder,
	aTypes: []*ffi.Type{
		&ffi.TypePointer,
		&ffi.TypePointer,
		&ffi.TypePointer,
	},
}, func(ctx context.Context, ffiCall ffiCall) func(*paimonTable, map[string]string) (*paimonReadBuilder, error) {
	return func(table *paimonTable, options map[string]string) (*paimonReadBuilder, error) {
		type paimonOption struct {
			key   *byte
			value *byte
		}
		opts := make([]paimonOption, 0, len(options))
		for key, value := range options {
			keyPtr, err := bytePtrFromString(key)
			if err != nil {
				return nil, err
			}
			valuePtr, err := bytePtrFromString(value)
			if err != nil {
				return nil, err
			}
			opts = append(opts, paimonOption{key: keyPtr, value: valuePtr})
		}
		var optsPtr unsafe.Pointer
		if len(opts) > 0 {
			optsPtr = unsafe.Pointer(&opts[0])
		}
		optsLen := uintptr(len(opts))
		var result resultReadBuilder
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&table),
			unsafe.Pointer(&optsPtr),
			unsafe.Pointer(&optsLen),
		)
		runtime.KeepAlive(opts)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.readBuilder, nil
	}
})
