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

var ffiTableNewPostponeFixedBucketWriteBuilder = newFFI(ffiOpts{
	sym:    "paimon_table_new_postpone_fixed_bucket_write_builder",
	rType:  &typeResultPostponeFixedBucketWriteBuilder,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(
	ctx context.Context,
	ffiCall ffiCall,
) func(*paimonTable) (*paimonPostponeFixedBucketWriteBuilder, error) {
	return func(table *paimonTable) (*paimonPostponeFixedBucketWriteBuilder, error) {
		var result resultPostponeFixedBucketWriteBuilder
		ffiCall(unsafe.Pointer(&result), unsafe.Pointer(&table))
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.writeBuilder, nil
	}
})

var ffiTableNewPostponeFixedBucketWriteBuilderWithCommitUser = newFFI(ffiOpts{
	sym:    "paimon_table_new_postpone_fixed_bucket_write_builder_with_commit_user",
	rType:  &typeResultPostponeFixedBucketWriteBuilder,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(
	ctx context.Context,
	ffiCall ffiCall,
) func(*paimonTable, string) (*paimonPostponeFixedBucketWriteBuilder, error) {
	return func(
		table *paimonTable,
		commitUser string,
	) (*paimonPostponeFixedBucketWriteBuilder, error) {
		commitUserPtr, err := bytePtrFromString(commitUser)
		if err != nil {
			return nil, err
		}
		var result resultPostponeFixedBucketWriteBuilder
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

var ffiPostponeFixedBucketWriteBuilderFree = newFFI(ffiOpts{
	sym:    "paimon_postpone_fixed_bucket_write_builder_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(
	_ context.Context,
	ffiCall ffiCall,
) func(*paimonPostponeFixedBucketWriteBuilder) {
	return func(builder *paimonPostponeFixedBucketWriteBuilder) {
		ffiCall(nil, unsafe.Pointer(&builder))
	}
})

var ffiPostponeFixedBucketWriteBuilderWithResources = newFFI(ffiOpts{
	sym:    "paimon_postpone_fixed_bucket_write_builder_with_resources",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(*paimonPostponeFixedBucketWriteBuilder, *paimonResourceContext) error {
	return func(builder *paimonPostponeFixedBucketWriteBuilder, resources *paimonResourceContext) error {
		var ffiError *paimonError
		ffiCall(unsafe.Pointer(&ffiError), unsafe.Pointer(&builder), unsafe.Pointer(&resources))
		return parseError(ctx, ffiError)
	}
})

var ffiPostponeFixedBucketWriteBuilderWithOverwrite = newFFI(ffiOpts{
	sym:    "paimon_postpone_fixed_bucket_write_builder_with_overwrite",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(
	ctx context.Context,
	ffiCall ffiCall,
) func(*paimonPostponeFixedBucketWriteBuilder) error {
	return func(builder *paimonPostponeFixedBucketWriteBuilder) error {
		var ffiError *paimonError
		ffiCall(unsafe.Pointer(&ffiError), unsafe.Pointer(&builder))
		return parseError(ctx, ffiError)
	}
})

var ffiPostponeFixedBucketWriteBuilderWithBucketPlan = newFFI(ffiOpts{
	sym:   "paimon_postpone_fixed_bucket_write_builder_with_bucket_plan",
	rType: &ffi.TypePointer,
	aTypes: []*ffi.Type{
		&ffi.TypePointer,
		&ffi.TypePointer,
		&ffi.TypePointer,
	},
}, func(
	ctx context.Context,
	ffiCall ffiCall,
) func(*paimonPostponeFixedBucketWriteBuilder, unsafe.Pointer, unsafe.Pointer) error {
	return func(
		builder *paimonPostponeFixedBucketWriteBuilder,
		array unsafe.Pointer,
		schema unsafe.Pointer,
	) error {
		var ffiError *paimonError
		ffiCall(
			unsafe.Pointer(&ffiError),
			unsafe.Pointer(&builder),
			unsafe.Pointer(&array),
			unsafe.Pointer(&schema),
		)
		return parseError(ctx, ffiError)
	}
})

var ffiPostponeFixedBucketWriteBuilderNewWrite = newFFI(ffiOpts{
	sym:    "paimon_postpone_fixed_bucket_write_builder_new_write",
	rType:  &typeResultPostponeFixedBucketTableWrite,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(
	ctx context.Context,
	ffiCall ffiCall,
) func(*paimonPostponeFixedBucketWriteBuilder) (*paimonPostponeFixedBucketTableWrite, error) {
	return func(
		builder *paimonPostponeFixedBucketWriteBuilder,
	) (*paimonPostponeFixedBucketTableWrite, error) {
		var result resultPostponeFixedBucketTableWrite
		ffiCall(unsafe.Pointer(&result), unsafe.Pointer(&builder))
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.write, nil
	}
})

var ffiPostponeFixedBucketWriteBuilderNewCommit = newFFI(ffiOpts{
	sym:    "paimon_postpone_fixed_bucket_write_builder_new_commit",
	rType:  &typeResultPostponeFixedBucketTableCommit,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(
	ctx context.Context,
	ffiCall ffiCall,
) func(*paimonPostponeFixedBucketWriteBuilder) (*paimonPostponeFixedBucketTableCommit, error) {
	return func(
		builder *paimonPostponeFixedBucketWriteBuilder,
	) (*paimonPostponeFixedBucketTableCommit, error) {
		var result resultPostponeFixedBucketTableCommit
		ffiCall(unsafe.Pointer(&result), unsafe.Pointer(&builder))
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.commit, nil
	}
})

var ffiPostponeFixedBucketTableWriteFree = newFFI(ffiOpts{
	sym:    "paimon_postpone_fixed_bucket_table_write_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(
	_ context.Context,
	ffiCall ffiCall,
) func(*paimonPostponeFixedBucketTableWrite) {
	return func(write *paimonPostponeFixedBucketTableWrite) {
		ffiCall(nil, unsafe.Pointer(&write))
	}
})

var ffiPostponeFixedBucketTableWriteWriteArrowBatch = newFFI(ffiOpts{
	sym:   "paimon_postpone_fixed_bucket_table_write_write_arrow_batch",
	rType: &ffi.TypePointer,
	aTypes: []*ffi.Type{
		&ffi.TypePointer,
		&ffi.TypePointer,
		&ffi.TypePointer,
	},
}, func(
	ctx context.Context,
	ffiCall ffiCall,
) func(*paimonPostponeFixedBucketTableWrite, unsafe.Pointer, unsafe.Pointer) error {
	return func(
		write *paimonPostponeFixedBucketTableWrite,
		array unsafe.Pointer,
		schema unsafe.Pointer,
	) error {
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

var ffiPostponeFixedBucketTableWritePrepareCommit = newFFI(ffiOpts{
	sym:    "paimon_postpone_fixed_bucket_table_write_prepare_commit",
	rType:  &typeResultPostponeFixedBucketPrepareCommit,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(
	ctx context.Context,
	ffiCall ffiCall,
) func(*paimonPostponeFixedBucketTableWrite) (*paimonPostponeFixedBucketCommitMessages, error) {
	return func(
		write *paimonPostponeFixedBucketTableWrite,
	) (*paimonPostponeFixedBucketCommitMessages, error) {
		var result resultPostponeFixedBucketPrepareCommit
		ffiCall(unsafe.Pointer(&result), unsafe.Pointer(&write))
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.messages, nil
	}
})

var ffiPostponeFixedBucketTableCommitFree = newFFI(ffiOpts{
	sym:    "paimon_postpone_fixed_bucket_table_commit_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(
	_ context.Context,
	ffiCall ffiCall,
) func(*paimonPostponeFixedBucketTableCommit) {
	return func(commit *paimonPostponeFixedBucketTableCommit) {
		ffiCall(nil, unsafe.Pointer(&commit))
	}
})

var ffiPostponeFixedBucketCommitMessagesFree = newFFI(ffiOpts{
	sym:    "paimon_postpone_fixed_bucket_commit_messages_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(
	_ context.Context,
	ffiCall ffiCall,
) func(*paimonPostponeFixedBucketCommitMessages) {
	return func(messages *paimonPostponeFixedBucketCommitMessages) {
		ffiCall(nil, unsafe.Pointer(&messages))
	}
})

var ffiPostponeFixedBucketCommitMessagesMerge = newFFI(ffiOpts{
	sym:    "paimon_postpone_fixed_bucket_commit_messages_merge",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(
	ctx context.Context,
	ffiCall ffiCall,
) func(*paimonPostponeFixedBucketCommitMessages, *paimonPostponeFixedBucketCommitMessages) error {
	return func(
		target *paimonPostponeFixedBucketCommitMessages,
		source *paimonPostponeFixedBucketCommitMessages,
	) error {
		var ffiError *paimonError
		ffiCall(
			unsafe.Pointer(&ffiError),
			unsafe.Pointer(&target),
			unsafe.Pointer(&source),
		)
		return parseError(ctx, ffiError)
	}
})

var ffiPostponeFixedBucketTableCommitCommit = newFixedCommitMessagesFFI(
	"paimon_postpone_fixed_bucket_table_commit_commit",
)
var ffiPostponeFixedBucketTableCommitCommitWithIdentifier = newFixedCommitMessagesIdentifierFFI(
	"paimon_postpone_fixed_bucket_table_commit_commit_with_identifier",
)
var ffiPostponeFixedBucketTableCommitFilterAndCommitWithIdentifier = newFixedCommitMessagesIdentifierFFI(
	"paimon_postpone_fixed_bucket_table_commit_filter_and_commit_with_identifier",
)
var ffiPostponeFixedBucketTableCommitAbort = newFixedCommitMessagesFFI(
	"paimon_postpone_fixed_bucket_table_commit_abort",
)

func newFixedCommitMessagesFFI(
	symbol contextKey,
) *FFI[func(
	*paimonPostponeFixedBucketTableCommit,
	*paimonPostponeFixedBucketCommitMessages,
) error] {
	return newFFI(ffiOpts{
		sym:    symbol,
		rType:  &ffi.TypePointer,
		aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
	}, func(
		ctx context.Context,
		ffiCall ffiCall,
	) func(
		*paimonPostponeFixedBucketTableCommit,
		*paimonPostponeFixedBucketCommitMessages,
	) error {
		return func(
			commit *paimonPostponeFixedBucketTableCommit,
			messages *paimonPostponeFixedBucketCommitMessages,
		) error {
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

func newFixedCommitMessagesIdentifierFFI(
	symbol contextKey,
) *FFI[func(
	*paimonPostponeFixedBucketTableCommit,
	*paimonPostponeFixedBucketCommitMessages,
	int64,
) error] {
	return newFFI(ffiOpts{
		sym:    symbol,
		rType:  &ffi.TypePointer,
		aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypeSint64},
	}, func(
		ctx context.Context,
		ffiCall ffiCall,
	) func(
		*paimonPostponeFixedBucketTableCommit,
		*paimonPostponeFixedBucketCommitMessages,
		int64,
	) error {
		return func(
			commit *paimonPostponeFixedBucketTableCommit,
			messages *paimonPostponeFixedBucketCommitMessages,
			identifier int64,
		) error {
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

var ffiPostponeFixedBucketTableCommitTruncateTable = newFFI(ffiOpts{
	sym:    "paimon_postpone_fixed_bucket_table_commit_truncate_table",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(
	ctx context.Context,
	ffiCall ffiCall,
) func(*paimonPostponeFixedBucketTableCommit) error {
	return func(commit *paimonPostponeFixedBucketTableCommit) error {
		var ffiError *paimonError
		ffiCall(unsafe.Pointer(&ffiError), unsafe.Pointer(&commit))
		return parseError(ctx, ffiError)
	}
})

var ffiPostponeFixedBucketTableCommitTruncateTableWithIdentifier = newFFI(ffiOpts{
	sym:    "paimon_postpone_fixed_bucket_table_commit_truncate_table_with_identifier",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypeSint64},
}, func(
	ctx context.Context,
	ffiCall ffiCall,
) func(*paimonPostponeFixedBucketTableCommit, int64) error {
	return func(commit *paimonPostponeFixedBucketTableCommit, identifier int64) error {
		var ffiError *paimonError
		ffiCall(
			unsafe.Pointer(&ffiError),
			unsafe.Pointer(&commit),
			unsafe.Pointer(&identifier),
		)
		return parseError(ctx, ffiError)
	}
})
