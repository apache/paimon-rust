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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package paimon

import (
	"context"
	"errors"
	"sync"
	"unsafe"

	"github.com/jupiterrider/ffi"
)

// ResourceContext shares a memory reservation budget across readers and writers.
// Reservations are accounting estimates, not a process memory limit.
type ResourceContext struct {
	ctx   context.Context
	lib   *libRef
	inner *paimonResourceContext
	mu    sync.RWMutex
}

// ResourceMetrics reports current and peak reserved bytes.
type ResourceMetrics struct {
	ReservedMemoryBytes     uintptr
	PeakReservedMemoryBytes uintptr
}

// NewResourceContext creates a shared reservation budget in bytes.
// A zero limit rejects nonempty reservations.
func NewResourceContext(memoryLimitBytes uintptr) (*ResourceContext, error) {
	ctx, lib, err := ensureLoaded()
	if err != nil {
		return nil, err
	}
	inner, err := ffiResourceContextCreate.symbol(ctx)(memoryLimitBytes)
	if err != nil {
		return nil, err
	}
	lib.acquire()
	return &ResourceContext{ctx: ctx, lib: lib, inner: inner}, nil
}

// Metrics reads the current and peak reservations. The counters are sampled independently.
func (r *ResourceContext) Metrics() (ResourceMetrics, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.inner == nil {
		return ResourceMetrics{}, ErrClosed
	}
	return ffiResourceContextMetrics.symbol(r.ctx)(r.inner)
}

// Close releases this handle. Builders retain their own resource context clones.
func (r *ResourceContext) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.inner == nil {
		return
	}
	ffiResourceContextFree.symbol(r.ctx)(r.inner)
	r.inner = nil
	r.lib.release()
}

var errNilResourceContext = errors.New("paimon: resource context must not be nil")

var ffiResourceContextCreate = newFFI(ffiOpts{
	sym:    "paimon_resource_context_create",
	rType:  &typeResultResourceContext,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(uintptr) (*paimonResourceContext, error) {
	return func(memoryLimitBytes uintptr) (*paimonResourceContext, error) {
		var result resultResourceContext
		ffiCall(unsafe.Pointer(&result), unsafe.Pointer(&memoryLimitBytes))
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.context, nil
	}
})

var ffiResourceContextMetrics = newFFI(ffiOpts{
	sym:    "paimon_resource_context_metrics",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(*paimonResourceContext) (ResourceMetrics, error) {
	return func(resource *paimonResourceContext) (ResourceMetrics, error) {
		var metrics paimonResourceMetrics
		metricsPtr := &metrics
		var ffiError *paimonError
		ffiCall(unsafe.Pointer(&ffiError), unsafe.Pointer(&resource), unsafe.Pointer(&metricsPtr))
		if err := parseError(ctx, ffiError); err != nil {
			return ResourceMetrics{}, err
		}
		return ResourceMetrics{metrics.reservedMemoryBytes, metrics.peakReservedMemoryBytes}, nil
	}
})

var ffiResourceContextFree = newFFI(ffiOpts{
	sym:    "paimon_resource_context_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(*paimonResourceContext) {
	return func(resource *paimonResourceContext) {
		ffiCall(nil, unsafe.Pointer(&resource))
	}
})
