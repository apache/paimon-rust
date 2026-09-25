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
	"sync"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
)

// PostponeFixedBucketWriteBuilder creates fixed-bucket writers for bucket=-2
// tables. A resolved bucket plan must be supplied before NewWrite.
type PostponeFixedBucketWriteBuilder struct {
	ctx       context.Context
	lib       *libRef
	inner     *paimonPostponeFixedBucketWriteBuilder
	closeOnce sync.Once
}

// NewPostponeFixedBucketWriteBuilder creates an explicitly selected
// fixed-bucket builder for a postpone table.
func (t *Table) NewPostponeFixedBucketWriteBuilder() (*PostponeFixedBucketWriteBuilder, error) {
	if t.inner == nil {
		return nil, ErrClosed
	}
	inner, err := ffiTableNewPostponeFixedBucketWriteBuilder.symbol(t.ctx)(t.inner)
	if err != nil {
		return nil, err
	}
	t.lib.acquire()
	return &PostponeFixedBucketWriteBuilder{ctx: t.ctx, lib: t.lib, inner: inner}, nil
}

// NewPostponeFixedBucketWriteBuilderWithCommitUser creates a fixed-bucket
// builder with a stable commit identity.
func (t *Table) NewPostponeFixedBucketWriteBuilderWithCommitUser(
	commitUser string,
) (*PostponeFixedBucketWriteBuilder, error) {
	if t.inner == nil {
		return nil, ErrClosed
	}
	inner, err := ffiTableNewPostponeFixedBucketWriteBuilderWithCommitUser.symbol(t.ctx)(
		t.inner,
		commitUser,
	)
	if err != nil {
		return nil, err
	}
	t.lib.acquire()
	return &PostponeFixedBucketWriteBuilder{ctx: t.ctx, lib: t.lib, inner: inner}, nil
}

// Close releases the builder resources. Safe to call multiple times.
func (wb *PostponeFixedBucketWriteBuilder) Close() {
	wb.closeOnce.Do(func() {
		ffiPostponeFixedBucketWriteBuilderFree.symbol(wb.ctx)(wb.inner)
		wb.inner = nil
		wb.lib.release()
	})
}

// WithResources shares the context's reservation budget with writers from this builder.
// The builder retains the context after the caller closes its handle.
func (wb *PostponeFixedBucketWriteBuilder) WithResources(resources *ResourceContext) error {
	if wb.inner == nil {
		return ErrClosed
	}
	if resources == nil {
		return errNilResourceContext
	}
	resources.mu.RLock()
	defer resources.mu.RUnlock()
	if resources.inner == nil {
		return ErrClosed
	}
	return ffiPostponeFixedBucketWriteBuilderWithResources.symbol(wb.ctx)(wb.inner, resources.inner)
}

// WithOverwrite enables overwrite mode for both writers and committers created
// by this builder.
func (wb *PostponeFixedBucketWriteBuilder) WithOverwrite() error {
	if wb.inner == nil {
		return ErrClosed
	}
	return ffiPostponeFixedBucketWriteBuilderWithOverwrite.symbol(wb.ctx)(wb.inner)
}

// WithBucketPlan sets a resolved partition-to-bucket-count plan. The plan must
// contain the table partition columns followed by a non-null Int32
// total_buckets column. The caller retains ownership of plan.
func (wb *PostponeFixedBucketWriteBuilder) WithBucketPlan(plan arrow.Record) error {
	if wb.inner == nil {
		return ErrClosed
	}
	return withOwnedArrowRecord(
		plan,
		"paimon: bucket plan must not be nil",
		func(array, schema unsafe.Pointer) error {
			return ffiPostponeFixedBucketWriteBuilderWithBucketPlan.symbol(wb.ctx)(
				wb.inner,
				array,
				schema,
			)
		},
	)
}

// NewWrite creates a fixed-bucket writer. WithBucketPlan must be called first.
func (wb *PostponeFixedBucketWriteBuilder) NewWrite() (*PostponeFixedBucketTableWrite, error) {
	if wb.inner == nil {
		return nil, ErrClosed
	}
	inner, err := ffiPostponeFixedBucketWriteBuilderNewWrite.symbol(wb.ctx)(wb.inner)
	if err != nil {
		return nil, err
	}
	wb.lib.acquire()
	return &PostponeFixedBucketTableWrite{ctx: wb.ctx, lib: wb.lib, inner: inner}, nil
}

// NewCommit creates a committer using this builder's commit identity and mode.
func (wb *PostponeFixedBucketWriteBuilder) NewCommit() (*PostponeFixedBucketTableCommit, error) {
	if wb.inner == nil {
		return nil, ErrClosed
	}
	inner, err := ffiPostponeFixedBucketWriteBuilderNewCommit.symbol(wb.ctx)(wb.inner)
	if err != nil {
		return nil, err
	}
	wb.lib.acquire()
	return &PostponeFixedBucketTableCommit{ctx: wb.ctx, lib: wb.lib, inner: inner}, nil
}

// PostponeFixedBucketTableWrite writes rows according to a resolved bucket plan.
type PostponeFixedBucketTableWrite struct {
	ctx       context.Context
	lib       *libRef
	inner     *paimonPostponeFixedBucketTableWrite
	closeOnce sync.Once
}

// Close releases the writer resources. Safe to call multiple times.
func (tw *PostponeFixedBucketTableWrite) Close() {
	tw.closeOnce.Do(func() {
		ffiPostponeFixedBucketTableWriteFree.symbol(tw.ctx)(tw.inner)
		tw.inner = nil
		tw.lib.release()
	})
}

// WriteArrowBatch writes one Arrow record batch. The caller retains ownership.
func (tw *PostponeFixedBucketTableWrite) WriteArrowBatch(record arrow.Record) error {
	if tw.inner == nil {
		return ErrClosed
	}
	return withOwnedArrowRecord(
		record,
		"paimon: record batch must not be nil",
		func(array, schema unsafe.Pointer) error {
			return ffiPostponeFixedBucketTableWriteWriteArrowBatch.symbol(tw.ctx)(
				tw.inner,
				array,
				schema,
			)
		},
	)
}

// PrepareCommit finalizes pending writes and returns fixed-bucket messages.
// It consumes the writer; subsequent operations return ErrClosed.
func (tw *PostponeFixedBucketTableWrite) PrepareCommit() (*PostponeFixedBucketCommitMessages, error) {
	if tw.inner == nil {
		return nil, ErrClosed
	}
	inner, err := ffiPostponeFixedBucketTableWritePrepareCommit.symbol(tw.ctx)(tw.inner)
	if err != nil {
		tw.Close()
		return nil, err
	}
	tw.lib.acquire()
	tw.Close()
	return &PostponeFixedBucketCommitMessages{ctx: tw.ctx, lib: tw.lib, inner: inner}, nil
}

// PostponeFixedBucketCommitMessages contains files produced by fixed-bucket
// writers. It is a process-local native handle and cannot be transferred
// between processes or passed to a standard TableCommit.
type PostponeFixedBucketCommitMessages struct {
	ctx       context.Context
	lib       *libRef
	inner     *paimonPostponeFixedBucketCommitMessages
	closeOnce sync.Once
}

// Close releases the messages. Safe to call multiple times.
func (m *PostponeFixedBucketCommitMessages) Close() {
	m.closeOnce.Do(func() {
		ffiPostponeFixedBucketCommitMessagesFree.symbol(m.ctx)(m.inner)
		m.inner = nil
		m.lib.release()
	})
}

// Merge appends a copy of source's messages. Both handles must belong to the
// same process, and both builders must use the same table, commit user, and
// overwrite mode.
func (m *PostponeFixedBucketCommitMessages) Merge(
	source *PostponeFixedBucketCommitMessages,
) error {
	if m.inner == nil {
		return ErrClosed
	}
	if source == nil {
		return errors.New("paimon: source messages must not be nil")
	}
	if source.inner == nil {
		return ErrClosed
	}
	return ffiPostponeFixedBucketCommitMessagesMerge.symbol(m.ctx)(m.inner, source.inner)
}

// PostponeFixedBucketTableCommit commits fixed-bucket messages using the mode
// selected on its builder.
type PostponeFixedBucketTableCommit struct {
	ctx       context.Context
	lib       *libRef
	inner     *paimonPostponeFixedBucketTableCommit
	closeOnce sync.Once
}

// Close releases the committer resources. Safe to call multiple times.
func (tc *PostponeFixedBucketTableCommit) Close() {
	tc.closeOnce.Do(func() {
		ffiPostponeFixedBucketTableCommitFree.symbol(tc.ctx)(tc.inner)
		tc.inner = nil
		tc.lib.release()
	})
}

func (tc *PostponeFixedBucketTableCommit) withMessages(
	messages *PostponeFixedBucketCommitMessages,
	operation func(
		*paimonPostponeFixedBucketTableCommit,
		*paimonPostponeFixedBucketCommitMessages,
	) error,
) error {
	if tc.inner == nil {
		return ErrClosed
	}
	if messages == nil || messages.inner == nil {
		return ErrClosed
	}
	return operation(tc.inner, messages.inner)
}

func (tc *PostponeFixedBucketTableCommit) withMessagesAndIdentifier(
	messages *PostponeFixedBucketCommitMessages,
	commitIdentifier int64,
	operation func(
		*paimonPostponeFixedBucketTableCommit,
		*paimonPostponeFixedBucketCommitMessages,
		int64,
	) error,
) error {
	if tc.inner == nil {
		return ErrClosed
	}
	if messages == nil || messages.inner == nil {
		return ErrClosed
	}
	return operation(tc.inner, messages.inner, commitIdentifier)
}

// Commit persists fixed-bucket messages using the builder's append or overwrite
// mode.
func (tc *PostponeFixedBucketTableCommit) Commit(
	messages *PostponeFixedBucketCommitMessages,
) error {
	return tc.withMessages(messages, ffiPostponeFixedBucketTableCommitCommit.symbol(tc.ctx))
}

// CommitWithIdentifier commits with a stable identifier.
func (tc *PostponeFixedBucketTableCommit) CommitWithIdentifier(
	messages *PostponeFixedBucketCommitMessages,
	commitIdentifier int64,
) error {
	return tc.withMessagesAndIdentifier(
		messages,
		commitIdentifier,
		ffiPostponeFixedBucketTableCommitCommitWithIdentifier.symbol(tc.ctx),
	)
}

// FilterAndCommitWithIdentifier makes a retry idempotent.
func (tc *PostponeFixedBucketTableCommit) FilterAndCommitWithIdentifier(
	messages *PostponeFixedBucketCommitMessages,
	commitIdentifier int64,
) error {
	return tc.withMessagesAndIdentifier(
		messages,
		commitIdentifier,
		ffiPostponeFixedBucketTableCommitFilterAndCommitWithIdentifier.symbol(tc.ctx),
	)
}

// TruncateTable removes all table data.
func (tc *PostponeFixedBucketTableCommit) TruncateTable() error {
	if tc.inner == nil {
		return ErrClosed
	}
	return ffiPostponeFixedBucketTableCommitTruncateTable.symbol(tc.ctx)(tc.inner)
}

// TruncateTableWithIdentifier removes all table data with a stable identifier.
func (tc *PostponeFixedBucketTableCommit) TruncateTableWithIdentifier(
	commitIdentifier int64,
) error {
	if tc.inner == nil {
		return ErrClosed
	}
	return ffiPostponeFixedBucketTableCommitTruncateTableWithIdentifier.symbol(tc.ctx)(
		tc.inner,
		commitIdentifier,
	)
}

// Abort performs best-effort cleanup of files in prepared messages.
func (tc *PostponeFixedBucketTableCommit) Abort(
	messages *PostponeFixedBucketCommitMessages,
) error {
	return tc.withMessages(messages, ffiPostponeFixedBucketTableCommitAbort.symbol(tc.ctx))
}
