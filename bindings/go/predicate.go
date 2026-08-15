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
	"math"
	"runtime"
	"sync"
	"unsafe"

	"github.com/jupiterrider/ffi"
)

// Datum type tags (must match paimon-c datum_from_c).
const (
	datumTagBool                int32 = 0
	datumTagTinyInt             int32 = 1
	datumTagSmallInt            int32 = 2
	datumTagInt                 int32 = 3
	datumTagLong                int32 = 4
	datumTagFloat               int32 = 5
	datumTagDouble              int32 = 6
	datumTagString              int32 = 7
	datumTagDate                int32 = 8
	datumTagTime                int32 = 9
	datumTagTimestamp           int32 = 10
	datumTagLocalZonedTimestamp int32 = 11
	datumTagDecimal             int32 = 12
	datumTagBytes               int32 = 13
)

// Datum is a typed literal value for predicate comparison.
// The internal representation is hidden to allow future changes
// (e.g. switching to opaque handles) without breaking callers.
type Datum struct {
	inner paimonDatumC
}

// BoolDatum creates a boolean datum.
func BoolDatum(v bool) Datum {
	var iv int64
	if v {
		iv = 1
	}
	return Datum{inner: paimonDatumC{tag: datumTagBool, intVal: iv}}
}

// TinyIntDatum creates a tinyint datum.
func TinyIntDatum(v int8) Datum {
	return Datum{inner: paimonDatumC{tag: datumTagTinyInt, intVal: int64(v)}}
}

// SmallIntDatum creates a smallint datum.
func SmallIntDatum(v int16) Datum {
	return Datum{inner: paimonDatumC{tag: datumTagSmallInt, intVal: int64(v)}}
}

// IntDatum creates an int datum.
func IntDatum(v int32) Datum {
	return Datum{inner: paimonDatumC{tag: datumTagInt, intVal: int64(v)}}
}

// LongDatum creates a long (bigint) datum.
func LongDatum(v int64) Datum {
	return Datum{inner: paimonDatumC{tag: datumTagLong, intVal: v}}
}

// FloatDatum creates a float datum.
func FloatDatum(v float32) Datum {
	return Datum{inner: paimonDatumC{tag: datumTagFloat, dblVal: float64(v)}}
}

// DoubleDatum creates a double datum.
func DoubleDatum(v float64) Datum {
	return Datum{inner: paimonDatumC{tag: datumTagDouble, dblVal: v}}
}

// StringDatum creates a string datum.
func StringDatum(v string) Datum {
	b := []byte(v)
	d := paimonDatumC{tag: datumTagString, strLen: uintptr(len(b))}
	if len(b) > 0 {
		d.strData = &b[0]
	}
	return Datum{inner: d}
}

// Date represents a date value as epoch days since 1970-01-01.
// Usage: pb.Eq("dt", paimon.Date(19000))
type Date int32

// Time represents a time-of-day value as milliseconds since midnight.
// Usage: pb.Eq("t", paimon.Time(3600000))
type Time int32

// Timestamp represents a timestamp without timezone (millis + sub-millis nanos).
// Usage: pb.Eq("ts", paimon.Timestamp{Millis: 1700000000000, Nanos: 0})
type Timestamp struct {
	Millis int64
	Nanos  int32
}

// LocalZonedTimestamp represents a timestamp with local timezone semantics.
// Usage: pb.Eq("lzts", paimon.LocalZonedTimestamp{Millis: 1700000000000, Nanos: 0})
type LocalZonedTimestamp struct {
	Millis int64
	Nanos  int32
}

// Decimal represents a fixed-precision decimal value up to DECIMAL(38, s).
//
// The unscaled value is stored as a little-endian i128 split into two int64
// halves: Lo (low 64 bits, unsigned interpretation) and Hi (high 64 bits,
// sign-extended). For values that fit in int64, use [NewDecimal].
//
// Usage:
//
//	paimon.NewDecimal(12345, 10, 2)          // 123.45 as DECIMAL(10,2)
//	paimon.Decimal{Lo: lo, Hi: hi, ...}      // full i128
type Decimal struct {
	Lo        int64 // low 64 bits of unscaled i128 (unsigned interpretation)
	Hi        int64 // high 64 bits of unscaled i128 (sign extension)
	Precision uint32
	Scale     uint32
}

// NewDecimal creates a Decimal from an int64 unscaled value.
// For unscaled values that exceed int64 range, construct Decimal directly
// with Lo/Hi fields.
func NewDecimal(unscaled int64, precision, scale uint32) Decimal {
	hi := int64(0)
	if unscaled < 0 {
		hi = -1
	}
	return Decimal{Lo: unscaled, Hi: hi, Precision: precision, Scale: scale}
}

// Bytes represents a binary value.
// Usage: pb.Eq("data", paimon.Bytes(someSlice))
type Bytes []byte

// toDatum converts a Go value to a Datum for predicate comparison.
//
// Supported Go types and their Paimon mappings:
//   - bool                 → Bool
//   - int8                 → TinyInt
//   - int16                → SmallInt
//   - int32                → Int
//   - int                  → Int (if fits int32) or Long
//   - int64                → Long
//   - float32              → Float
//   - float64              → Double
//   - string               → String
//   - Date                 → Date
//   - Time                 → Time
//   - Timestamp            → Timestamp
//   - LocalZonedTimestamp  → LocalZonedTimestamp
//   - Decimal              → Decimal
//   - Bytes                → Bytes
//   - Datum                → passed through
func toDatum(v any) (Datum, error) {
	switch val := v.(type) {
	case bool:
		return BoolDatum(val), nil
	case int8:
		return TinyIntDatum(val), nil
	case int16:
		return SmallIntDatum(val), nil
	case int32:
		return IntDatum(val), nil
	case int:
		if val >= math.MinInt32 && val <= math.MaxInt32 {
			return IntDatum(int32(val)), nil
		}
		return LongDatum(int64(val)), nil
	case int64:
		return LongDatum(val), nil
	case float32:
		return FloatDatum(val), nil
	case float64:
		return DoubleDatum(val), nil
	case string:
		return StringDatum(val), nil
	case Date:
		return Datum{inner: paimonDatumC{tag: datumTagDate, intVal: int64(val)}}, nil
	case Time:
		return Datum{inner: paimonDatumC{tag: datumTagTime, intVal: int64(val)}}, nil
	case Timestamp:
		return Datum{inner: paimonDatumC{tag: datumTagTimestamp, intVal: val.Millis, intVal2: int64(val.Nanos)}}, nil
	case LocalZonedTimestamp:
		return Datum{inner: paimonDatumC{tag: datumTagLocalZonedTimestamp, intVal: val.Millis, intVal2: int64(val.Nanos)}}, nil
	case Decimal:
		return Datum{inner: paimonDatumC{
			tag: datumTagDecimal, intVal: val.Lo, intVal2: val.Hi,
			uintVal: val.Precision, uintVal2: val.Scale,
		}}, nil
	case Bytes:
		d := paimonDatumC{tag: datumTagBytes, strLen: uintptr(len(val))}
		if len(val) > 0 {
			d.strData = &val[0]
		}
		return Datum{inner: d}, nil
	case Datum:
		return val, nil
	default:
		return Datum{}, fmt.Errorf("unsupported datum type: %T", v)
	}
}

// Predicate is an opaque filter predicate for scan planning.
type Predicate struct {
	ctx       context.Context
	lib       *libRef
	inner     *paimonPredicate
	closeOnce sync.Once
}

// Close releases the predicate resources. Safe to call multiple times.
// Note: predicates passed to WithFilter or combinators (And/Or/Not) are consumed
// and should NOT be closed by the caller.
func (p *Predicate) Close() {
	p.closeOnce.Do(func() {
		if p.inner != nil {
			ffiPredicateFree.symbol(p.ctx)(p.inner)
			p.inner = nil
			p.lib.release()
		}
	})
}

// errConsumedPredicate is returned when a consumed or nil predicate is reused.
var errConsumedPredicate = fmt.Errorf("paimon: predicate already consumed or nil")

// PredicateBuilder creates filter predicates for a table.
// It holds a Go-level reference to the Table and does not own any C resources,
// so there is no Close() method.
type PredicateBuilder struct {
	table *Table
}

// Eq creates an equality predicate: column = value.
func (pb *PredicateBuilder) Eq(column string, value any) (*Predicate, error) {
	datum, err := toDatum(value)
	if err != nil {
		return nil, err
	}
	return pb.buildLeafPredicate(ffiPredicateEqual, column, datum)
}

// NotEq creates a not-equal predicate: column != value.
func (pb *PredicateBuilder) NotEq(column string, value any) (*Predicate, error) {
	datum, err := toDatum(value)
	if err != nil {
		return nil, err
	}
	return pb.buildLeafPredicate(ffiPredicateNotEqual, column, datum)
}

// Lt creates a less-than predicate: column < value.
func (pb *PredicateBuilder) Lt(column string, value any) (*Predicate, error) {
	datum, err := toDatum(value)
	if err != nil {
		return nil, err
	}
	return pb.buildLeafPredicate(ffiPredicateLessThan, column, datum)
}

// Le creates a less-or-equal predicate: column <= value.
func (pb *PredicateBuilder) Le(column string, value any) (*Predicate, error) {
	datum, err := toDatum(value)
	if err != nil {
		return nil, err
	}
	return pb.buildLeafPredicate(ffiPredicateLessOrEqual, column, datum)
}

// Gt creates a greater-than predicate: column > value.
func (pb *PredicateBuilder) Gt(column string, value any) (*Predicate, error) {
	datum, err := toDatum(value)
	if err != nil {
		return nil, err
	}
	return pb.buildLeafPredicate(ffiPredicateGreaterThan, column, datum)
}

// Ge creates a greater-or-equal predicate: column >= value.
func (pb *PredicateBuilder) Ge(column string, value any) (*Predicate, error) {
	datum, err := toDatum(value)
	if err != nil {
		return nil, err
	}
	return pb.buildLeafPredicate(ffiPredicateGreaterOrEqual, column, datum)
}

// IsNull creates an IS NULL predicate.
func (pb *PredicateBuilder) IsNull(column string) (*Predicate, error) {
	return pb.buildNullPredicate(ffiPredicateIsNull, column)
}

// IsNotNull creates an IS NOT NULL predicate.
func (pb *PredicateBuilder) IsNotNull(column string) (*Predicate, error) {
	return pb.buildNullPredicate(ffiPredicateIsNotNull, column)
}

// In creates an IN predicate: column IN (values...).
func (pb *PredicateBuilder) In(column string, values ...any) (*Predicate, error) {
	return pb.buildInPredicate(ffiPredicateIsIn, column, values)
}

// NotIn creates a NOT IN predicate: column NOT IN (values...).
func (pb *PredicateBuilder) NotIn(column string, values ...any) (*Predicate, error) {
	return pb.buildInPredicate(ffiPredicateIsNotIn, column, values)
}

// buildLeafPredicate is a helper for comparison predicates that take (table, column, datum).
func (pb *PredicateBuilder) buildLeafPredicate(
	ffiVar *FFI[func(*paimonTable, *byte, paimonDatumC) (*paimonPredicate, error)],
	column string, datum Datum,
) (*Predicate, error) {
	t := pb.table
	if t.inner == nil {
		return nil, ErrClosed
	}
	createFn := ffiVar.symbol(t.ctx)
	cCol := append([]byte(column), 0)
	inner, err := createFn(t.inner, &cCol[0], datum.inner)
	runtime.KeepAlive(cCol)
	runtime.KeepAlive(datum)
	if err != nil {
		return nil, err
	}
	t.lib.acquire()
	return &Predicate{ctx: t.ctx, lib: t.lib, inner: inner}, nil
}

// buildNullPredicate is a helper for IS NULL / IS NOT NULL predicates.
func (pb *PredicateBuilder) buildNullPredicate(
	ffiVar *FFI[func(*paimonTable, *byte) (*paimonPredicate, error)],
	column string,
) (*Predicate, error) {
	t := pb.table
	if t.inner == nil {
		return nil, ErrClosed
	}
	createFn := ffiVar.symbol(t.ctx)
	cCol := append([]byte(column), 0)
	inner, err := createFn(t.inner, &cCol[0])
	runtime.KeepAlive(cCol)
	if err != nil {
		return nil, err
	}
	t.lib.acquire()
	return &Predicate{ctx: t.ctx, lib: t.lib, inner: inner}, nil
}

// buildInPredicate is a helper for IS IN / IS NOT IN predicates.
func (pb *PredicateBuilder) buildInPredicate(
	ffiVar *FFI[func(*paimonTable, *byte, unsafe.Pointer, uintptr) (*paimonPredicate, error)],
	column string, values []any,
) (*Predicate, error) {
	t := pb.table
	if t.inner == nil {
		return nil, ErrClosed
	}
	datums := make([]paimonDatumC, len(values))
	for i, v := range values {
		d, err := toDatum(v)
		if err != nil {
			return nil, err
		}
		datums[i] = d.inner
	}
	createFn := ffiVar.symbol(t.ctx)
	cCol := append([]byte(column), 0)
	var datumsPtr unsafe.Pointer
	if len(datums) > 0 {
		datumsPtr = unsafe.Pointer(&datums[0])
	}
	inner, err := createFn(t.inner, &cCol[0], datumsPtr, uintptr(len(datums)))
	runtime.KeepAlive(cCol)
	runtime.KeepAlive(datums)
	runtime.KeepAlive(values)
	if err != nil {
		return nil, err
	}
	t.lib.acquire()
	return &Predicate{ctx: t.ctx, lib: t.lib, inner: inner}, nil
}

// combinePredicate is a shared helper for And/Or.
func (p *Predicate) combinePredicate(
	other *Predicate,
	ffiVar *FFI[func(*paimonPredicate, *paimonPredicate) *paimonPredicate],
) (*Predicate, error) {
	if p == nil || p.inner == nil {
		return nil, errConsumedPredicate
	}
	if other == nil || other.inner == nil {
		return nil, errConsumedPredicate
	}
	if p == other {
		return nil, fmt.Errorf("paimon: cannot combine a predicate with itself")
	}
	combineFn := ffiVar.symbol(p.ctx)
	p.inner = combineFn(p.inner, other.inner)
	other.inner = nil
	other.lib.release()
	return p, nil
}

// And combines this predicate with another using AND. Consumes both predicates
// (callers must NOT close either after this call).
func (p *Predicate) And(other *Predicate) (*Predicate, error) {
	return p.combinePredicate(other, ffiPredicateAnd)
}

// Or combines this predicate with another using OR. Consumes both predicates
// (callers must NOT close either after this call).
func (p *Predicate) Or(other *Predicate) (*Predicate, error) {
	return p.combinePredicate(other, ffiPredicateOr)
}

// Not negates this predicate. Consumes the input
// (caller must NOT close it after this call).
func (p *Predicate) Not() (*Predicate, error) {
	if p == nil || p.inner == nil {
		return nil, errConsumedPredicate
	}
	negateFn := ffiPredicateNot.symbol(p.ctx)
	p.inner = negateFn(p.inner)
	return p, nil
}

// FFI wrappers for predicate functions.

var ffiPredicateFree = newFFI(ffiOpts{
	sym:    "paimon_predicate_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(p *paimonPredicate) {
	return func(p *paimonPredicate) {
		ffiCall(nil, unsafe.Pointer(&p))
	}
})

var ffiPredicateEqual = newPredicateLeafFFI("paimon_predicate_equal")
var ffiPredicateNotEqual = newPredicateLeafFFI("paimon_predicate_not_equal")
var ffiPredicateLessThan = newPredicateLeafFFI("paimon_predicate_less_than")
var ffiPredicateLessOrEqual = newPredicateLeafFFI("paimon_predicate_less_or_equal")
var ffiPredicateGreaterThan = newPredicateLeafFFI("paimon_predicate_greater_than")
var ffiPredicateGreaterOrEqual = newPredicateLeafFFI("paimon_predicate_greater_or_equal")

// newPredicateLeafFFI creates an FFI wrapper for comparison predicate functions
// with signature: (table, column, datum) -> result_predicate.
func newPredicateLeafFFI(sym string) *FFI[func(*paimonTable, *byte, paimonDatumC) (*paimonPredicate, error)] {
	return newFFI(ffiOpts{
		sym:    contextKey(sym),
		rType:  &typeResultPredicate,
		aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &typePaimonDatum},
	}, func(ctx context.Context, ffiCall ffiCall) func(*paimonTable, *byte, paimonDatumC) (*paimonPredicate, error) {
		return func(table *paimonTable, column *byte, datum paimonDatumC) (*paimonPredicate, error) {
			var result resultPredicate
			ffiCall(
				unsafe.Pointer(&result),
				unsafe.Pointer(&table),
				unsafe.Pointer(&column),
				unsafe.Pointer(&datum),
			)
			if result.error != nil {
				return nil, parseError(ctx, result.error)
			}
			return result.predicate, nil
		}
	})
}

var ffiPredicateIsNull = newPredicateNullFFI("paimon_predicate_is_null")
var ffiPredicateIsNotNull = newPredicateNullFFI("paimon_predicate_is_not_null")

// newPredicateNullFFI creates an FFI wrapper for null-check predicate functions
// with signature: (table, column) -> result_predicate.
func newPredicateNullFFI(sym string) *FFI[func(*paimonTable, *byte) (*paimonPredicate, error)] {
	return newFFI(ffiOpts{
		sym:    contextKey(sym),
		rType:  &typeResultPredicate,
		aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
	}, func(ctx context.Context, ffiCall ffiCall) func(*paimonTable, *byte) (*paimonPredicate, error) {
		return func(table *paimonTable, column *byte) (*paimonPredicate, error) {
			var result resultPredicate
			ffiCall(
				unsafe.Pointer(&result),
				unsafe.Pointer(&table),
				unsafe.Pointer(&column),
			)
			if result.error != nil {
				return nil, parseError(ctx, result.error)
			}
			return result.predicate, nil
		}
	})
}

var ffiPredicateIsIn = newPredicateInFFI("paimon_predicate_is_in")
var ffiPredicateIsNotIn = newPredicateInFFI("paimon_predicate_is_not_in")

// newPredicateInFFI creates an FFI wrapper for IN/NOT IN predicate functions
// with signature: (table, column, datums, datums_len) -> result_predicate.
func newPredicateInFFI(sym string) *FFI[func(*paimonTable, *byte, unsafe.Pointer, uintptr) (*paimonPredicate, error)] {
	return newFFI(ffiOpts{
		sym:    contextKey(sym),
		rType:  &typeResultPredicate,
		aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
	}, func(ctx context.Context, ffiCall ffiCall) func(*paimonTable, *byte, unsafe.Pointer, uintptr) (*paimonPredicate, error) {
		return func(table *paimonTable, column *byte, datums unsafe.Pointer, datumsLen uintptr) (*paimonPredicate, error) {
			var result resultPredicate
			ffiCall(
				unsafe.Pointer(&result),
				unsafe.Pointer(&table),
				unsafe.Pointer(&column),
				unsafe.Pointer(&datums),
				unsafe.Pointer(&datumsLen),
			)
			if result.error != nil {
				return nil, parseError(ctx, result.error)
			}
			return result.predicate, nil
		}
	})
}

var ffiPredicateAnd = newFFI(ffiOpts{
	sym:    "paimon_predicate_and",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(*paimonPredicate, *paimonPredicate) *paimonPredicate {
	return func(a, b *paimonPredicate) *paimonPredicate {
		var result *paimonPredicate
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&a),
			unsafe.Pointer(&b),
		)
		return result
	}
})

var ffiPredicateOr = newFFI(ffiOpts{
	sym:    "paimon_predicate_or",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(*paimonPredicate, *paimonPredicate) *paimonPredicate {
	return func(a, b *paimonPredicate) *paimonPredicate {
		var result *paimonPredicate
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&a),
			unsafe.Pointer(&b),
		)
		return result
	}
})

var ffiPredicateNot = newFFI(ffiOpts{
	sym:    "paimon_predicate_not",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(*paimonPredicate) *paimonPredicate {
	return func(p *paimonPredicate) *paimonPredicate {
		var result *paimonPredicate
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&p),
		)
		return result
	}
})
