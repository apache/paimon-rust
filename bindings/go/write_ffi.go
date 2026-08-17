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
	"unsafe"

	"github.com/jupiterrider/ffi"
)

var ffiTableNewWriteBuilder = newFFI(ffiOpts{
	sym:    "paimon_table_new_write_builder",
	rType:  &typeResultWriteBuilder,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(*paimonTable) (*paimonWriteBuilder, error) {
	return func(table *paimonTable) (*paimonWriteBuilder, error) {
		var result resultWriteBuilder
		ffiCall(unsafe.Pointer(&result), unsafe.Pointer(&table))
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.writeBuilder, nil
	}
})

var ffiTableNewWriteBuilderWithCommitUser = newFFI(ffiOpts{
	sym:    "paimon_table_new_write_builder_with_commit_user",
	rType:  &typeResultWriteBuilder,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(*paimonTable, string) (*paimonWriteBuilder, error) {
	return func(table *paimonTable, commitUser string) (*paimonWriteBuilder, error) {
		commitUserPtr, err := bytePtrFromString(commitUser)
		if err != nil {
			return nil, err
		}
		var result resultWriteBuilder
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&table),
			unsafe.Pointer(&commitUserPtr),
		)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.writeBuilder, nil
	}
})

var ffiWriteBuilderFree = newFFI(ffiOpts{
	sym:    "paimon_write_builder_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(*paimonWriteBuilder) {
	return func(builder *paimonWriteBuilder) {
		ffiCall(nil, unsafe.Pointer(&builder))
	}
})

var ffiWriteBuilderWithOverwrite = newFFI(ffiOpts{
	sym:    "paimon_write_builder_with_overwrite",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(*paimonWriteBuilder) error {
	return func(builder *paimonWriteBuilder) error {
		var ffiError *paimonError
		ffiCall(unsafe.Pointer(&ffiError), unsafe.Pointer(&builder))
		return parseError(ctx, ffiError)
	}
})

var ffiWriteBuilderNewWrite = newFFI(ffiOpts{
	sym:    "paimon_write_builder_new_write",
	rType:  &typeResultTableWrite,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(*paimonWriteBuilder) (*paimonTableWrite, error) {
	return func(builder *paimonWriteBuilder) (*paimonTableWrite, error) {
		var result resultTableWrite
		ffiCall(unsafe.Pointer(&result), unsafe.Pointer(&builder))
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.write, nil
	}
})

var ffiTableWriteFree = newFFI(ffiOpts{
	sym:    "paimon_table_write_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(*paimonTableWrite) {
	return func(write *paimonTableWrite) {
		ffiCall(nil, unsafe.Pointer(&write))
	}
})

var ffiTableWriteWriteArrowBatch = newFFI(ffiOpts{
	sym:    "paimon_table_write_write_arrow_batch",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(*paimonTableWrite, unsafe.Pointer, unsafe.Pointer) error {
	return func(write *paimonTableWrite, array unsafe.Pointer, schema unsafe.Pointer) error {
		var ffiError *paimonError
		ffiCall(
			unsafe.Pointer(&ffiError),
			unsafe.Pointer(&write),
			unsafe.Pointer(&array),
			unsafe.Pointer(&schema),
		)
		return parseError(ctx, ffiError)
	}
})

var ffiTableWritePrepareCommit = newFFI(ffiOpts{
	sym:    "paimon_table_write_prepare_commit",
	rType:  &typeResultPrepareCommit,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(*paimonTableWrite) (*paimonCommitMessages, error) {
	return func(write *paimonTableWrite) (*paimonCommitMessages, error) {
		var result resultPrepareCommit
		ffiCall(unsafe.Pointer(&result), unsafe.Pointer(&write))
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.messages, nil
	}
})

var ffiWriteBuilderNewCommit = newFFI(ffiOpts{
	sym:    "paimon_write_builder_new_commit",
	rType:  &typeResultTableCommit,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(*paimonWriteBuilder) (*paimonTableCommit, error) {
	return func(builder *paimonWriteBuilder) (*paimonTableCommit, error) {
		var result resultTableCommit
		ffiCall(unsafe.Pointer(&result), unsafe.Pointer(&builder))
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.commit, nil
	}
})

var ffiTableCommitFree = newFFI(ffiOpts{
	sym:    "paimon_table_commit_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(*paimonTableCommit) {
	return func(commit *paimonTableCommit) {
		ffiCall(nil, unsafe.Pointer(&commit))
	}
})

var ffiCommitMessagesFree = newFFI(ffiOpts{
	sym:    "paimon_commit_messages_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(*paimonCommitMessages) {
	return func(messages *paimonCommitMessages) {
		ffiCall(nil, unsafe.Pointer(&messages))
	}
})

var ffiCommitMessagesMerge = newFFI(ffiOpts{
	sym:    "paimon_commit_messages_merge",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(*paimonCommitMessages, *paimonCommitMessages) error {
	return func(target *paimonCommitMessages, source *paimonCommitMessages) error {
		var ffiError *paimonError
		ffiCall(
			unsafe.Pointer(&ffiError),
			unsafe.Pointer(&target),
			unsafe.Pointer(&source),
		)
		return parseError(ctx, ffiError)
	}
})

var ffiTableCommitCommit = newCommitMessagesFFI("paimon_table_commit_commit")
var ffiTableCommitCommitWithIdentifier = newCommitMessagesIdentifierFFI(
	"paimon_table_commit_commit_with_identifier",
)
var ffiTableCommitFilterAndCommitWithIdentifier = newCommitMessagesIdentifierFFI(
	"paimon_table_commit_filter_and_commit_with_identifier",
)
var ffiTableCommitOverwrite = newCommitMessagesFFI("paimon_table_commit_overwrite")
var ffiTableCommitOverwriteWithIdentifier = newCommitMessagesIdentifierFFI(
	"paimon_table_commit_overwrite_with_identifier",
)
var ffiTableCommitAbort = newCommitMessagesFFI("paimon_table_commit_abort")

func newCommitMessagesFFI(symbol contextKey) *FFI[func(*paimonTableCommit, *paimonCommitMessages) error] {
	return newFFI(ffiOpts{
		sym:    symbol,
		rType:  &ffi.TypePointer,
		aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
	}, func(ctx context.Context, ffiCall ffiCall) func(*paimonTableCommit, *paimonCommitMessages) error {
		return func(commit *paimonTableCommit, messages *paimonCommitMessages) error {
			var ffiError *paimonError
			ffiCall(
				unsafe.Pointer(&ffiError),
				unsafe.Pointer(&commit),
				unsafe.Pointer(&messages),
			)
			return parseError(ctx, ffiError)
		}
	})
}

func newCommitMessagesIdentifierFFI(
	symbol contextKey,
) *FFI[func(*paimonTableCommit, *paimonCommitMessages, int64) error] {
	return newFFI(ffiOpts{
		sym:    symbol,
		rType:  &ffi.TypePointer,
		aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypeSint64},
	}, func(ctx context.Context, ffiCall ffiCall) func(*paimonTableCommit, *paimonCommitMessages, int64) error {
		return func(commit *paimonTableCommit, messages *paimonCommitMessages, identifier int64) error {
			var ffiError *paimonError
			ffiCall(
				unsafe.Pointer(&ffiError),
				unsafe.Pointer(&commit),
				unsafe.Pointer(&messages),
				unsafe.Pointer(&identifier),
			)
			return parseError(ctx, ffiError)
		}
	})
}

var ffiTableCommitTruncateTable = newCommitFFI("paimon_table_commit_truncate_table")
var ffiTableCommitTruncateTableWithIdentifier = newCommitWithIdentifierFFI(
	"paimon_table_commit_truncate_table_with_identifier",
)

func newCommitFFI(symbol contextKey) *FFI[func(*paimonTableCommit) error] {
	return newFFI(ffiOpts{
		sym:    symbol,
		rType:  &ffi.TypePointer,
		aTypes: []*ffi.Type{&ffi.TypePointer},
	}, func(ctx context.Context, ffiCall ffiCall) func(*paimonTableCommit) error {
		return func(commit *paimonTableCommit) error {
			var ffiError *paimonError
			ffiCall(unsafe.Pointer(&ffiError), unsafe.Pointer(&commit))
			return parseError(ctx, ffiError)
		}
	})
}

func newCommitWithIdentifierFFI(
	symbol contextKey,
) *FFI[func(*paimonTableCommit, int64) error] {
	return newFFI(ffiOpts{
		sym:    symbol,
		rType:  &ffi.TypePointer,
		aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypeSint64},
	}, func(ctx context.Context, ffiCall ffiCall) func(*paimonTableCommit, int64) error {
		return func(commit *paimonTableCommit, identifier int64) error {
			var ffiError *paimonError
			ffiCall(
				unsafe.Pointer(&ffiError),
				unsafe.Pointer(&commit),
				unsafe.Pointer(&identifier),
			)
			return parseError(ctx, ffiError)
		}
	})
}
