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
	"unsafe"

	"github.com/jupiterrider/ffi"
)

// FFI type definitions mirroring C repr structs from paimon-c.
var (
	// Result types: { value, *error }
	// paimon_result_catalog_new { catalog: paimon_catalog, error: *paimon_error }
	typeResultCatalogNew = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	// paimon_result_get_table { table: paimon_table, error: *paimon_error }
	typeResultGetTable = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	// paimon_result_identifier_new { identifier: paimon_identifier, error: *paimon_error }
	typeResultIdentifierNew = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	// paimon_result_new_read { read: paimon_table_read, error: *paimon_error }
	typeResultNewRead = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	// paimon_result_read_builder { read_builder: paimon_read_builder, error: *paimon_error }
	typeResultReadBuilder = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	// paimon_result_table_scan { scan: paimon_table_scan, error: *paimon_error }
	typeResultTableScan = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	// paimon_result_plan { plan: paimon_plan, error: *paimon_error }
	typeResultPlan = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	// paimon_result_record_batch_reader { reader: *paimon_record_batch_reader, error: *paimon_error }
	typeResultRecordBatchReader = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	// paimon_arrow_batch { array: *c_void, schema: *c_void }
	typeArrowBatch = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	// paimon_result_predicate { predicate: *paimon_predicate, error: *paimon_error }
	typeResultPredicate = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	// Write result types all have the layout { opaque pointer, *paimon_error }.
	typeResultWriteBuilder = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	typeResultTableWrite = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	typeResultTableCommit = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	typeResultPrepareCommit = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	// paimon_datum { tag: i32, int_val: i64, double_val: f64, str_data: *u8, str_len: usize,
	//                int_val2: i64, uint_val: u32, uint_val2: u32 }
	typePaimonDatum = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypeSint32,  // tag
			&ffi.TypeSint32,  // padding
			&ffi.TypeSint64,  // int_val
			&ffi.TypeDouble,  // double_val
			&ffi.TypePointer, // str_data
			&ffi.TypePointer, // str_len (usize)
			&ffi.TypeSint64,  // int_val2
			&ffi.TypeUint32,  // uint_val
			&ffi.TypeUint32,  // uint_val2
			nil,
		}[0],
	}

	// paimon_result_next_batch { batch: paimon_arrow_batch, error: *paimon_error }
	typeResultNextBatch = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer, // batch.array
			&ffi.TypePointer, // batch.schema
			&ffi.TypePointer, // error
			nil,
		}[0],
	}
)

// Go mirror structs for C types.

type paimonBytes struct {
	data *byte
	len  uintptr
}

type paimonError struct {
	code    int32
	message paimonBytes
}

// Opaque pointer wrappers
type paimonCatalog struct{}
type paimonIdentifier struct{}
type paimonTable struct{}
type paimonReadBuilder struct{}
type paimonTableScan struct{}
type paimonTableRead struct{}
type paimonPlan struct{}
type paimonRecordBatchReader struct{}
type paimonPredicate struct{}
type paimonWriteBuilder struct{}
type paimonTableWrite struct{}
type paimonTableCommit struct{}
type paimonCommitMessages struct{}

// Result types matching the C repr structs
type resultCatalogNew struct {
	catalog *paimonCatalog
	error   *paimonError
}

type resultGetTable struct {
	table *paimonTable
	error *paimonError
}

type resultIdentifierNew struct {
	identifier *paimonIdentifier
	error      *paimonError
}

type resultNewRead struct {
	read  *paimonTableRead
	error *paimonError
}

type resultReadBuilder struct {
	readBuilder *paimonReadBuilder
	error       *paimonError
}

type resultTableScan struct {
	scan  *paimonTableScan
	error *paimonError
}

type resultPlan struct {
	plan  *paimonPlan
	error *paimonError
}

type resultRecordBatchReader struct {
	reader *paimonRecordBatchReader
	error  *paimonError
}

type resultPredicate struct {
	predicate *paimonPredicate
	error     *paimonError
}

type resultWriteBuilder struct {
	writeBuilder *paimonWriteBuilder
	error        *paimonError
}

type resultTableWrite struct {
	write *paimonTableWrite
	error *paimonError
}

type resultTableCommit struct {
	commit *paimonTableCommit
	error  *paimonError
}

type resultPrepareCommit struct {
	messages *paimonCommitMessages
	error    *paimonError
}

// paimonDatumC mirrors the C paimon_datum struct.
type paimonDatumC struct {
	tag      int32
	_pad0    [4]byte // padding for alignment
	intVal   int64
	dblVal   float64
	strData  *byte
	strLen   uintptr
	intVal2  int64
	uintVal  uint32
	uintVal2 uint32
}

// arrowBatch holds a single Arrow record batch via the Arrow C Data Interface.
type arrowBatch struct {
	ctx      context.Context
	lib      *libRef
	array    unsafe.Pointer
	schema   unsafe.Pointer
	released bool
}

func (b *arrowBatch) release() {
	if b.released {
		return
	}
	b.released = true
	runtime.SetFinalizer(b, nil)
	ffiArrowBatchFree.symbol(b.ctx)(b.array, b.schema)
	b.lib.release()
}

type resultNextBatch struct {
	array  unsafe.Pointer
	schema unsafe.Pointer
	error  *paimonError
}

func parseBytes(b paimonBytes) []byte {
	if b.len == 0 {
		return nil
	}
	data := make([]byte, b.len)
	copy(data, unsafe.Slice(b.data, b.len))
	return data
}
