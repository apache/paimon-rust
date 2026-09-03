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
	"fmt"
	"runtime"
	"strings"
	"sync"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/jupiterrider/ffi"
)

// BlobReader resolves serialized BlobDescriptors.
type BlobReader struct {
	ctx   context.Context
	lib   *libRef
	inner *paimonBlobReader
	mu    sync.RWMutex
}

// NewBlobReader creates a descriptor reader with FileIO options.
func NewBlobReader(storageOptions map[string]string) (*BlobReader, error) {
	ctx, lib, err := ensureLoaded()
	if err != nil {
		return nil, err
	}
	inner, err := ffiBlobReaderNew.symbol(ctx)(storageOptions)
	if err != nil {
		return nil, err
	}
	lib.acquire()
	return &BlobReader{ctx: ctx, lib: lib, inner: inner}, nil
}

// NewBlobReader creates a descriptor reader using this table's FileIO.
func (t *Table) NewBlobReader() (*BlobReader, error) {
	if t.inner == nil {
		return nil, ErrClosed
	}
	inner, err := ffiTableNewBlobReader.symbol(t.ctx)(t.inner)
	if err != nil {
		return nil, err
	}
	t.lib.acquire()
	return &BlobReader{ctx: t.ctx, lib: t.lib, inner: inner}, nil
}

// ReadBlob resolves one descriptor.
func (r *BlobReader) ReadBlob(descriptor []byte) ([]byte, error) {
	values, err := r.ReadBlobs([][]byte{descriptor})
	if err != nil {
		return nil, err
	}
	return values[0], nil
}

// ReadBlobs resolves a batch in input order.
func (r *BlobReader) ReadBlobs(descriptors [][]byte) ([][]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.inner == nil {
		return nil, ErrClosed
	}
	return ffiBlobReaderReadBlobs.symbol(r.ctx)(r.inner, descriptors)
}

// StringBlobMapDescriptors returns one MAP<STRING, BLOB> row.
func StringBlobMapDescriptors(column arrow.Array, row int) (map[string][]byte, error) {
	m, ok := column.(*array.Map)
	if !ok {
		return nil, fmt.Errorf("paimon: BLOB map column is %T, want *array.Map", column)
	}
	if row < 0 || row >= m.Len() {
		return nil, fmt.Errorf("paimon: BLOB map row %d is out of range", row)
	}
	if m.IsNull(row) {
		return nil, nil
	}
	keys, ok := m.Keys().(*array.String)
	if !ok {
		return nil, fmt.Errorf("paimon: BLOB map keys are %T, want *array.String", m.Keys())
	}
	descriptors, ok := m.Items().(*array.Binary)
	if !ok {
		return nil, fmt.Errorf("paimon: BLOB map values are %T, want *array.Binary", m.Items())
	}
	start, end := m.ValueOffsets(row)
	result := make(map[string][]byte, end-start)
	for index := start; index < end; index++ {
		i := int(index)
		key := strings.Clone(keys.Value(i))
		if descriptors.IsNull(i) {
			result[key] = nil
			continue
		}
		result[key] = append([]byte(nil), descriptors.Value(i)...)
	}
	return result, nil
}

// Close releases the reader and is idempotent.
func (r *BlobReader) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.inner == nil {
		return
	}
	ffiBlobReaderFree.symbol(r.ctx)(r.inner)
	r.inner = nil
	r.lib.release()
}

var ffiBlobReaderNew = newFFI(ffiOpts{
	sym:    "paimon_blob_reader_new",
	rType:  &typeResultBlobReader,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(map[string]string) (*paimonBlobReader, error) {
	return func(options map[string]string) (*paimonBlobReader, error) {
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
		var result resultBlobReader
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&optsPtr),
			unsafe.Pointer(&optsLen),
		)
		runtime.KeepAlive(opts)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.reader, nil
	}
})

var ffiTableNewBlobReader = newFFI(ffiOpts{
	sym:    "paimon_table_new_blob_reader",
	rType:  &typeResultBlobReader,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(*paimonTable) (*paimonBlobReader, error) {
	return func(table *paimonTable) (*paimonBlobReader, error) {
		var result resultBlobReader
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&table),
		)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.reader, nil
	}
})

var ffiBlobReaderReadBlobs = newFFI(ffiOpts{
	sym:   "paimon_blob_reader_read_blobs",
	rType: &typeResultReadBlobs,
	aTypes: []*ffi.Type{
		&ffi.TypePointer,
		&ffi.TypePointer,
		&ffi.TypePointer,
	},
}, func(ctx context.Context, ffiCall ffiCall) func(*paimonBlobReader, [][]byte) ([][]byte, error) {
	return func(reader *paimonBlobReader, descriptors [][]byte) ([][]byte, error) {
		slices := make([]paimonByteSlice, len(descriptors))
		for index, descriptor := range descriptors {
			if len(descriptor) > 0 {
				slices[index].data = &descriptor[0]
			}
			slices[index].len = uintptr(len(descriptor))
		}
		var slicesPtr unsafe.Pointer
		if len(slices) > 0 {
			slicesPtr = unsafe.Pointer(&slices[0])
		}
		slicesLen := uintptr(len(slices))
		var result resultReadBlobs
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&reader),
			unsafe.Pointer(&slicesPtr),
			unsafe.Pointer(&slicesLen),
		)
		runtime.KeepAlive(descriptors)
		runtime.KeepAlive(slices)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		defer ffiBytesArrayFree.symbol(ctx)(result.blobs)
		if result.blobs.len > 0 && result.blobs.data == nil {
			return nil, fmt.Errorf("paimon: native BlobReader returned a null result array")
		}

		values := make([][]byte, result.blobs.len)
		for index, value := range unsafe.Slice(result.blobs.data, result.blobs.len) {
			if value.len == 0 {
				values[index] = []byte{}
			} else {
				values[index] = parseBytes(value)
			}
		}
		return values, nil
	}
})

var ffiBlobReaderFree = newFFI(ffiOpts{
	sym:    "paimon_blob_reader_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(*paimonBlobReader) {
	return func(reader *paimonBlobReader) {
		ffiCall(nil, unsafe.Pointer(&reader))
	}
})

var ffiBytesArrayFree = newFFI(ffiOpts{
	sym:    "paimon_bytes_array_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&typePaimonBytesArray},
}, func(_ context.Context, ffiCall ffiCall) func(paimonBytesArray) {
	return func(values paimonBytesArray) {
		ffiCall(nil, unsafe.Pointer(&values))
	}
})
