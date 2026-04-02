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

// Package paimon provides a Go binding for Apache Paimon Rust.
//
// This binding uses purego and libffi to call into the paimon-c shared library.
// The pre-built shared library is embedded in the package and automatically
// loaded at runtime — no manual build step needed.
//
// This package requires CGO because it imports the arrow-go cdata package
// for Arrow C Data Interface support.
//
// Basic usage:
//
//	// Create a catalog with options
//	catalog, err := paimon.NewCatalog(map[string]string{
//		"warehouse": "/path/to/warehouse",
//	})
//	if err != nil { log.Fatal(err) }
//	defer catalog.Close()
//
//	table, err := catalog.GetTable(paimon.NewIdentifier("default", "my_table"))
//	if err != nil { log.Fatal(err) }
//	defer table.Close()
//
// For S3 or OSS warehouses, pass the appropriate credentials:
//
//	// S3
//	catalog, _ := paimon.NewCatalog(map[string]string{
//		"warehouse":            "s3://bucket/warehouse",
//		"s3.access-key-id":     "...",
//		"s3.secret-access-key": "...",
//		"s3.region":            "us-east-1",
//	})
//
//	// OSS
//	catalog, _ := paimon.NewCatalog(map[string]string{
//		"warehouse":            "oss://bucket/warehouse",
//		"fs.oss.accessKeyId":     "...",
//		"fs.oss.accessKeySecret": "...",
//		"fs.oss.endpoint":        "oss-cn-hangzhou.aliyuncs.com",
//	})
package paimon

import (
	"context"
	"sync"
)

var (
	globalOnce sync.Once
	globalCtx  context.Context
	globalLib  *libRef
	globalErr  error
)

func ensureLoaded() (context.Context, *libRef, error) {
	globalOnce.Do(func() {
		if err := loadEmbeddedLib(); err != nil {
			globalErr = err
			return
		}
		globalCtx, globalLib, globalErr = newContext(libPath)
	})
	return globalCtx, globalLib, globalErr
}
