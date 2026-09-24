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
	"fmt"
	"unsafe"

	"github.com/jupiterrider/ffi"
)

// ErrClosed is returned when an operation is attempted on a closed resource.
var ErrClosed = errors.New("paimon: use of closed resource")

// ErrorCode represents categories of errors from paimon.
type ErrorCode int32

const (
	CodeUnexpected        ErrorCode = 0
	CodeUnsupported       ErrorCode = 1
	CodeNotFound          ErrorCode = 2
	CodeAlreadyExist      ErrorCode = 3
	CodeInvalidInput      ErrorCode = 4
	CodeIoError           ErrorCode = 5
	CodeResourceExhausted ErrorCode = 6
)

func parseError(ctx context.Context, err *paimonError) error {
	if err == nil {
		return nil
	}
	defer ffiErrorFree.symbol(ctx)(err)
	return &Error{
		code:    ErrorCode(err.code),
		message: string(parseBytes(err.message)),
	}
}

// Error represents a paimon error with code and message.
type Error struct {
	code    ErrorCode
	message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("paimon error(%d): %s", e.code, e.message)
}

// Code returns the error code.
func (e *Error) Code() ErrorCode {
	return e.code
}

// Message returns the error message.
func (e *Error) Message() string {
	return e.message
}

var ffiErrorFree = newFFI(ffiOpts{
	sym:    "paimon_error_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(e *paimonError) {
	return func(e *paimonError) {
		ffiCall(
			nil,
			unsafe.Pointer(&e),
		)
	}
})
