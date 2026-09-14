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
	"encoding/json"
	"runtime"
	"unsafe"

	"github.com/jupiterrider/ffi"
)

// CommitKind identifies the change represented by a snapshot.
type CommitKind string

const (
	CommitKindAppend    CommitKind = "APPEND"
	CommitKindCompact   CommitKind = "COMPACT"
	CommitKindOverwrite CommitKind = "OVERWRITE"
	CommitKindAnalyze   CommitKind = "ANALYZE"
)

// Snapshot describes a Paimon table snapshot.
type Snapshot struct {
	Version                   int32             `json:"version"`
	ID                        int64             `json:"id"`
	SchemaID                  int64             `json:"schemaId"`
	BaseManifestList          string            `json:"baseManifestList"`
	DeltaManifestList         string            `json:"deltaManifestList"`
	ChangelogManifestList     *string           `json:"changelogManifestList,omitempty"`
	ChangelogManifestListSize *int64            `json:"changelogManifestListSize,omitempty"`
	IndexManifest             *string           `json:"indexManifest,omitempty"`
	CommitUser                string            `json:"commitUser"`
	CommitIdentifier          int64             `json:"commitIdentifier"`
	CommitKind                CommitKind        `json:"commitKind"`
	TimeMillis                int64             `json:"timeMillis"`
	LogOffsets                map[int32]int64   `json:"logOffsets,omitempty"`
	TotalRecordCount          *int64            `json:"totalRecordCount,omitempty"`
	DeltaRecordCount          *int64            `json:"deltaRecordCount,omitempty"`
	ChangelogRecordCount      *int64            `json:"changelogRecordCount,omitempty"`
	Watermark                 *int64            `json:"watermark,omitempty"`
	Statistics                *string           `json:"statistics,omitempty"`
	Properties                map[string]string `json:"properties,omitempty"`
	NextRowID                 *int64            `json:"nextRowId,omitempty"`
}

// Tag describes a named table snapshot.
type Tag struct {
	Name             string   `json:"tagName"`
	Snapshot         Snapshot `json:"snapshot"`
	CreateTimeMillis *int64   `json:"tagCreateTime,omitempty"`
	TimeRetained     *string  `json:"tagTimeRetained,omitempty"`
}

// LatestSnapshot returns the latest snapshot, or nil when the table is empty.
func (t *Table) LatestSnapshot() (*Snapshot, error) {
	if t.inner == nil {
		return nil, ErrClosed
	}
	return ffiTableLatestSnapshot.symbol(t.ctx)(t.inner)
}

// CreateTag creates a tag for snapshotID, or the latest snapshot when snapshotID is nil.
func (c *Catalog) CreateTag(
	id Identifier,
	tagName string,
	snapshotID *int64,
	ignoreIfExists bool,
) error {
	if c.inner == nil {
		return ErrClosed
	}
	cID, err := c.newIdentifier(id)
	if err != nil {
		return err
	}
	defer ffiIdentifierFree.symbol(c.ctx)(cID)
	return ffiCatalogCreateTag.symbol(c.ctx)(
		c.inner,
		cID,
		tagName,
		snapshotID,
		ignoreIfExists,
	)
}

// GetTag returns a tag and its snapshot metadata.
func (c *Catalog) GetTag(id Identifier, tagName string) (Tag, error) {
	if c.inner == nil {
		return Tag{}, ErrClosed
	}
	cID, err := c.newIdentifier(id)
	if err != nil {
		return Tag{}, err
	}
	defer ffiIdentifierFree.symbol(c.ctx)(cID)
	return ffiCatalogGetTag.symbol(c.ctx)(c.inner, cID, tagName)
}

// DeleteTag deletes a tag.
func (c *Catalog) DeleteTag(id Identifier, tagName string, ignoreIfNotExists bool) error {
	if c.inner == nil {
		return ErrClosed
	}
	cID, err := c.newIdentifier(id)
	if err != nil {
		return err
	}
	defer ffiIdentifierFree.symbol(c.ctx)(cID)
	return ffiCatalogDeleteTag.symbol(c.ctx)(c.inner, cID, tagName, ignoreIfNotExists)
}

var ffiCatalogCreateTag = newFFI(ffiOpts{
	sym:   "paimon_catalog_create_tag",
	rType: &ffi.TypePointer,
	aTypes: []*ffi.Type{
		&ffi.TypePointer,
		&ffi.TypePointer,
		&ffi.TypePointer,
		&ffi.TypePointer,
		&ffi.TypeUint8,
	},
}, func(ctx context.Context, ffiCall ffiCall) func(
	*paimonCatalog,
	*paimonIdentifier,
	string,
	*int64,
	bool,
) error {
	return func(
		catalog *paimonCatalog,
		id *paimonIdentifier,
		tagName string,
		snapshotID *int64,
		ignoreIfExists bool,
	) error {
		tagNamePtr, err := bytePtrFromString(tagName)
		if err != nil {
			return err
		}
		var snapshotIDPtr unsafe.Pointer
		if snapshotID != nil {
			snapshotIDPtr = unsafe.Pointer(snapshotID)
		}
		ignore := uint8(0)
		if ignoreIfExists {
			ignore = 1
		}
		var ffiError *paimonError
		ffiCall(
			unsafe.Pointer(&ffiError),
			unsafe.Pointer(&catalog),
			unsafe.Pointer(&id),
			unsafe.Pointer(&tagNamePtr),
			unsafe.Pointer(&snapshotIDPtr),
			unsafe.Pointer(&ignore),
		)
		runtime.KeepAlive(tagNamePtr)
		runtime.KeepAlive(snapshotID)
		return parseError(ctx, ffiError)
	}
})

var ffiTableLatestSnapshot = newFFI(ffiOpts{
	sym:    "paimon_table_latest_snapshot",
	rType:  &typeResultLatestSnapshot,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(*paimonTable) (*Snapshot, error) {
	return func(table *paimonTable) (*Snapshot, error) {
		var result resultLatestSnapshot
		ffiCall(unsafe.Pointer(&result), unsafe.Pointer(&table))
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		defer ffiBytesFree.symbol(ctx)(result.snapshot)
		var snapshot *Snapshot
		if err := json.Unmarshal(parseBytes(result.snapshot), &snapshot); err != nil {
			return nil, err
		}
		return snapshot, nil
	}
})

var ffiCatalogGetTag = newFFI(ffiOpts{
	sym:    "paimon_catalog_get_tag",
	rType:  &typeResultGetTag,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(
	*paimonCatalog,
	*paimonIdentifier,
	string,
) (Tag, error) {
	return func(catalog *paimonCatalog, id *paimonIdentifier, tagName string) (Tag, error) {
		tagNamePtr, err := bytePtrFromString(tagName)
		if err != nil {
			return Tag{}, err
		}
		var result resultGetTag
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&catalog),
			unsafe.Pointer(&id),
			unsafe.Pointer(&tagNamePtr),
		)
		runtime.KeepAlive(tagNamePtr)
		if result.error != nil {
			return Tag{}, parseError(ctx, result.error)
		}
		defer ffiBytesFree.symbol(ctx)(result.tag)
		var tag Tag
		if err := json.Unmarshal(parseBytes(result.tag), &tag); err != nil {
			return Tag{}, err
		}
		return tag, nil
	}
})

var ffiCatalogDeleteTag = newFFI(ffiOpts{
	sym:   "paimon_catalog_delete_tag",
	rType: &ffi.TypePointer,
	aTypes: []*ffi.Type{
		&ffi.TypePointer,
		&ffi.TypePointer,
		&ffi.TypePointer,
		&ffi.TypeUint8,
	},
}, func(ctx context.Context, ffiCall ffiCall) func(
	*paimonCatalog,
	*paimonIdentifier,
	string,
	bool,
) error {
	return func(
		catalog *paimonCatalog,
		id *paimonIdentifier,
		tagName string,
		ignoreIfNotExists bool,
	) error {
		tagNamePtr, err := bytePtrFromString(tagName)
		if err != nil {
			return err
		}
		ignore := uint8(0)
		if ignoreIfNotExists {
			ignore = 1
		}
		var ffiError *paimonError
		ffiCall(
			unsafe.Pointer(&ffiError),
			unsafe.Pointer(&catalog),
			unsafe.Pointer(&id),
			unsafe.Pointer(&tagNamePtr),
			unsafe.Pointer(&ignore),
		)
		runtime.KeepAlive(tagNamePtr)
		return parseError(ctx, ffiError)
	}
})

var ffiBytesFree = newFFI(ffiOpts{
	sym:    "paimon_bytes_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&typePaimonBytes},
}, func(_ context.Context, ffiCall ffiCall) func(paimonBytes) {
	return func(value paimonBytes) {
		ffiCall(nil, unsafe.Pointer(&value))
	}
})
