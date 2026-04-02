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
	"sync"
	"unsafe"

	"github.com/jupiterrider/ffi"
)

// Catalog wraps a paimon Catalog.
type Catalog struct {
	ctx       context.Context
	lib       *libRef
	inner     *paimonCatalog
	closeOnce sync.Once
}

// NewCatalog creates a new Catalog using the CatalogFactory with the given options.
// The catalog type is determined by the "metastore" option (default: "filesystem").
//
// Common options:
//   - "warehouse": The warehouse path (required)
//   - "metastore": Catalog type - "filesystem" (default) or "rest"
//   - "uri": REST catalog server URI (required for REST catalog)
//   - "s3.access-key-id", "s3.secret-access-key", "s3.region": S3 credentials
//   - "fs.oss.accessKeyId", "fs.oss.accessKeySecret", "fs.oss.endpoint": OSS credentials
func NewCatalog(options map[string]string) (*Catalog, error) {
	ctx, lib, err := ensureLoaded()
	if err != nil {
		return nil, err
	}
	createFn := ffiCatalogCreate.symbol(ctx)
	inner, err := createFn(options)
	if err != nil {
		return nil, err
	}
	lib.acquire()
	return &Catalog{ctx: ctx, lib: lib, inner: inner}, nil
}

// Close releases the catalog resources. Safe to call multiple times.
func (c *Catalog) Close() {
	c.closeOnce.Do(func() {
		ffiCatalogFree.symbol(c.ctx)(c.inner)
		c.inner = nil
		c.lib.release()
	})
}

// GetTable retrieves a table from the catalog using the given identifier.
func (c *Catalog) GetTable(id Identifier) (*Table, error) {
	if c.inner == nil {
		return nil, ErrClosed
	}
	createIdFn := ffiIdentifierNew.symbol(c.ctx)
	cID, err := createIdFn(id.database, id.object)
	if err != nil {
		return nil, err
	}
	defer ffiIdentifierFree.symbol(c.ctx)(cID)

	getFn := ffiCatalogGetTable.symbol(c.ctx)
	inner, err := getFn(c.inner, cID)
	if err != nil {
		return nil, err
	}
	c.lib.acquire()
	return &Table{ctx: c.ctx, lib: c.lib, inner: inner}, nil
}

var ffiCatalogCreate = newFFI(ffiOpts{
	sym:    "paimon_catalog_create",
	rType:  &typeResultCatalogNew,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(options map[string]string) (*paimonCatalog, error) {
	return func(options map[string]string) (*paimonCatalog, error) {
		// Convert map to array of paimonOption
		type paimonOption struct {
			key   *byte
			value *byte
		}
		opts := make([]paimonOption, 0, len(options))
		for k, v := range options {
			keyBytes, err := bytePtrFromString(k)
			if err != nil {
				return nil, err
			}
			valBytes, err := bytePtrFromString(v)
			if err != nil {
				return nil, err
			}
			opts = append(opts, paimonOption{key: keyBytes, value: valBytes})
		}

		var optsPtr unsafe.Pointer
		if len(opts) > 0 {
			optsPtr = unsafe.Pointer(&opts[0])
		}
		optsLen := uintptr(len(opts))

		var result resultCatalogNew
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&optsPtr),
			unsafe.Pointer(&optsLen),
		)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.catalog, nil
	}
})

var ffiCatalogFree = newFFI(ffiOpts{
	sym:    "paimon_catalog_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(catalog *paimonCatalog) {
	return func(catalog *paimonCatalog) {
		ffiCall(
			nil,
			unsafe.Pointer(&catalog),
		)
	}
})

var ffiIdentifierNew = newFFI(ffiOpts{
	sym:    "paimon_identifier_new",
	rType:  &typeResultIdentifierNew,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(database, object string) (*paimonIdentifier, error) {
	return func(database, object string) (*paimonIdentifier, error) {
		byteDB, err := bytePtrFromString(database)
		if err != nil {
			return nil, err
		}
		byteObj, err := bytePtrFromString(object)
		if err != nil {
			return nil, err
		}
		var result resultIdentifierNew
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&byteDB),
			unsafe.Pointer(&byteObj),
		)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.identifier, nil
	}
})

var ffiIdentifierFree = newFFI(ffiOpts{
	sym:    "paimon_identifier_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(id *paimonIdentifier) {
	return func(id *paimonIdentifier) {
		ffiCall(
			nil,
			unsafe.Pointer(&id),
		)
	}
})

var ffiCatalogGetTable = newFFI(ffiOpts{
	sym:    "paimon_catalog_get_table",
	rType:  &typeResultGetTable,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(catalog *paimonCatalog, id *paimonIdentifier) (*paimonTable, error) {
	return func(catalog *paimonCatalog, id *paimonIdentifier) (*paimonTable, error) {
		var result resultGetTable
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&catalog),
			unsafe.Pointer(&id),
		)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.table, nil
	}
})
