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

	"github.com/apache/arrow-go/v18/arrow"
)

// WriteBuilder creates writers and committers that share one commit identity.
type WriteBuilder struct {
	ctx       context.Context
	lib       *libRef
	inner     *paimonWriteBuilder
	overwrite bool
	closeOnce sync.Once
}

// NewWriteBuilder creates a write builder with a generated commit identity.
func (t *Table) NewWriteBuilder() (*WriteBuilder, error) {
	if t.inner == nil {
		return nil, ErrClosed
	}
	inner, err := ffiTableNewWriteBuilder.symbol(t.ctx)(t.inner)
	if err != nil {
		return nil, err
	}
	t.lib.acquire()
	return &WriteBuilder{ctx: t.ctx, lib: t.lib, inner: inner}, nil
}

// NewWriteBuilderWithCommitUser creates a write builder with a stable commit
// identity for merging messages across writers and for identifier retries.
func (t *Table) NewWriteBuilderWithCommitUser(commitUser string) (*WriteBuilder, error) {
	if t.inner == nil {
		return nil, ErrClosed
	}
	inner, err := ffiTableNewWriteBuilderWithCommitUser.symbol(t.ctx)(t.inner, commitUser)
	if err != nil {
		return nil, err
	}
	t.lib.acquire()
	return &WriteBuilder{ctx: t.ctx, lib: t.lib, inner: inner}, nil
}

// Close releases the write builder resources. Safe to call multiple times.
func (wb *WriteBuilder) Close() {
	wb.closeOnce.Do(func() {
		ffiWriteBuilderFree.symbol(wb.ctx)(wb.inner)
		wb.inner = nil
		wb.lib.release()
	})
}

// WithOverwrite enables overwrite mode for this builder's writers and committers.
func (wb *WriteBuilder) WithOverwrite() error {
	if wb.inner == nil {
		return ErrClosed
	}
	if err := ffiWriteBuilderWithOverwrite.symbol(wb.ctx)(wb.inner); err != nil {
		return err
	}
	wb.overwrite = true
	return nil
}

// NewWrite creates a writer that accumulates Arrow record batches.
func (wb *WriteBuilder) NewWrite() (*TableWrite, error) {
	if wb.inner == nil {
		return nil, ErrClosed
	}
	inner, err := ffiWriteBuilderNewWrite.symbol(wb.ctx)(wb.inner)
	if err != nil {
		return nil, err
	}
	wb.lib.acquire()
	return &TableWrite{ctx: wb.ctx, lib: wb.lib, inner: inner}, nil
}

// NewCommit creates a committer that shares this builder's identity and mode.
func (wb *WriteBuilder) NewCommit() (*TableCommit, error) {
	if wb.inner == nil {
		return nil, ErrClosed
	}
	inner, err := ffiWriteBuilderNewCommit.symbol(wb.ctx)(wb.inner)
	if err != nil {
		return nil, err
	}
	wb.lib.acquire()
	return &TableCommit{ctx: wb.ctx, lib: wb.lib, inner: inner, overwrite: wb.overwrite}, nil
}

// TableWrite accumulates Arrow record batches until PrepareCommit is called.
type TableWrite struct {
	ctx       context.Context
	lib       *libRef
	inner     *paimonTableWrite
	closeOnce sync.Once
}

// Close releases the writer resources. Unprepared data is discarded. Safe to
// call multiple times.
func (tw *TableWrite) Close() {
	tw.closeOnce.Do(func() {
		ffiTableWriteFree.symbol(tw.ctx)(tw.inner)
		tw.inner = nil
		tw.lib.release()
	})
}

// WriteArrowBatch writes one record batch whose schema must match the table
// schema. The record remains owned by the caller.
func (tw *TableWrite) WriteArrowBatch(record arrow.Record) error {
	if tw.inner == nil {
		return ErrClosed
	}
	return withOwnedArrowRecord(
		record,
		"paimon: record batch must not be nil",
		func(array, schema unsafe.Pointer) error {
			return ffiTableWriteWriteArrowBatch.symbol(tw.ctx)(tw.inner, array, schema)
		},
	)
}

// PrepareCommit returns pending writes as commit messages; the writer can be reused.
func (tw *TableWrite) PrepareCommit() (*CommitMessages, error) {
	if tw.inner == nil {
		return nil, ErrClosed
	}
	inner, err := ffiTableWritePrepareCommit.symbol(tw.ctx)(tw.inner)
	if err != nil {
		return nil, err
	}
	tw.lib.acquire()
	return &CommitMessages{ctx: tw.ctx, lib: tw.lib, inner: inner}, nil
}

// CommitMessages contains the files produced by one or more writers. It is a
// process-local native handle and cannot be transferred between processes.
type CommitMessages struct {
	ctx       context.Context
	lib       *libRef
	inner     *paimonCommitMessages
	closeOnce sync.Once
}

// Close releases the commit messages. Safe to call multiple times.
func (m *CommitMessages) Close() {
	m.closeOnce.Do(func() {
		ffiCommitMessagesFree.symbol(m.ctx)(m.inner)
		m.inner = nil
		m.lib.release()
	})
}

// Merge appends a copy of source's messages; both handles stay valid and must
// share a table and commit user. It does not establish fixed-bucket ownership.
func (m *CommitMessages) Merge(source *CommitMessages) error {
	if m.inner == nil {
		return ErrClosed
	}
	if source == nil || source.inner == nil {
		return ErrClosed
	}
	return ffiCommitMessagesMerge.symbol(m.ctx)(m.inner, source.inner)
}

// TableCommit persists or aborts prepared commit messages.
type TableCommit struct {
	ctx       context.Context
	lib       *libRef
	inner     *paimonTableCommit
	overwrite bool
	closeOnce sync.Once
}

// Close releases the committer resources. Safe to call multiple times.
func (tc *TableCommit) Close() {
	tc.closeOnce.Do(func() {
		ffiTableCommitFree.symbol(tc.ctx)(tc.inner)
		tc.inner = nil
		tc.lib.release()
	})
}

func (tc *TableCommit) withMessages(
	messages *CommitMessages,
	operation func(*paimonTableCommit, *paimonCommitMessages) error,
) error {
	if tc.inner == nil {
		return ErrClosed
	}
	if messages == nil || messages.inner == nil {
		return ErrClosed
	}
	return operation(tc.inner, messages.inner)
}

func (tc *TableCommit) withMessagesAndIdentifier(
	messages *CommitMessages,
	commitIdentifier int64,
	operation func(*paimonTableCommit, *paimonCommitMessages, int64) error,
) error {
	if tc.inner == nil {
		return ErrClosed
	}
	if messages == nil || messages.inner == nil {
		return ErrClosed
	}
	return operation(tc.inner, messages.inner, commitIdentifier)
}

// Commit persists messages using the builder's append or overwrite mode.
func (tc *TableCommit) Commit(messages *CommitMessages) error {
	operation := ffiTableCommitCommit.symbol(tc.ctx)
	if tc.overwrite {
		operation = ffiTableCommitOverwrite.symbol(tc.ctx)
	}
	return tc.withMessages(messages, operation)
}

// CommitWithIdentifier commits with a monotonically increasing identifier.
func (tc *TableCommit) CommitWithIdentifier(messages *CommitMessages, commitIdentifier int64) error {
	operation := ffiTableCommitCommitWithIdentifier.symbol(tc.ctx)
	if tc.overwrite {
		operation = ffiTableCommitOverwriteWithIdentifier.symbol(tc.ctx)
	}
	return tc.withMessagesAndIdentifier(
		messages,
		commitIdentifier,
		operation,
	)
}

// FilterAndCommitWithIdentifier skips messages already committed under
// commitIdentifier, making retries idempotent. Identifier commits always
// filter in overwrite mode.
func (tc *TableCommit) FilterAndCommitWithIdentifier(
	messages *CommitMessages,
	commitIdentifier int64,
) error {
	operation := ffiTableCommitFilterAndCommitWithIdentifier.symbol(tc.ctx)
	if tc.overwrite {
		operation = ffiTableCommitOverwriteWithIdentifier.symbol(tc.ctx)
	}
	return tc.withMessagesAndIdentifier(
		messages,
		commitIdentifier,
		operation,
	)
}

// TruncateTable removes all table data.
func (tc *TableCommit) TruncateTable() error {
	if tc.inner == nil {
		return ErrClosed
	}
	return ffiTableCommitTruncateTable.symbol(tc.ctx)(tc.inner)
}

// TruncateTableWithIdentifier truncates with a stable commit identifier.
func (tc *TableCommit) TruncateTableWithIdentifier(commitIdentifier int64) error {
	if tc.inner == nil {
		return ErrClosed
	}
	return ffiTableCommitTruncateTableWithIdentifier.symbol(tc.ctx)(tc.inner, commitIdentifier)
}

// Abort performs best-effort cleanup of files created for a prepared commit.
func (tc *TableCommit) Abort(messages *CommitMessages) error {
	return tc.withMessages(messages, ffiTableCommitAbort.symbol(tc.ctx))
}
