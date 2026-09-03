<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Go Binding Release Checklist

Use this checklist when publishing the Go module at:

```text
github.com/apache/paimon-rust/bindings/go
```

## Tagging Strategy

- Choose a reviewed Rust release baseline commit first.
- That baseline usually corresponds to the Rust source release tag or an approved
  release-candidate commit.
- Build the Go binding release artifacts from that baseline.
- Because the Go module embeds generated shared-library archives,
  `bindings/go/vX.Y.Z` does not need to point to the exact same commit as the
  Rust release tag.
- Instead, `bindings/go/vX.Y.Z` may point to a follow-up repository commit that
  adds only Go release assets under `bindings/go/`.

In practice, the release relationship should look like this:

1. pick a Rust release baseline commit,
2. create the Rust release tag from that baseline when appropriate,
3. build the Go embedded libraries from the same baseline,
4. create a Go release commit that contains the generated `libpaimon_c.*.zst`
   files, and
5. tag that commit as `bindings/go/vX.Y.Z` or `bindings/go/vX.Y.Z-rcN`.

This keeps a clear link to the Rust release baseline while allowing the Go
submodule tag to contain the embedded artifacts required by Go module consumers.

## Recommended Sequence

- For an ASF-style release process, first prepare the Rust source release.
- Pushing a root Rust RC tag automatically publishes the Go RC convenience tag
  from the same source commit. After the RC is approved, pushing the final root
  tag does the same for the final Go version.
- For a manual recovery or standalone Go release, use the same reviewed Rust
  release commit or immutable tag as `source_ref`.

## Before Release

- Confirm the Go API changes are ready to publish.
- Confirm the target version is decided, for example `v0.1.0`.
- Confirm the Rust release baseline commit for this Go release is recorded.
- Decide the exact `source_ref` to pass into the release workflow.
- Confirm the base version in `version` matches the Paimon workspace version at
  `source_ref`; after `main` is bumped, it cannot be used to publish the previous version.
- Confirm whether this Go release is based on a Rust GA tag or an approved Rust
  release-candidate commit.
- Use `vX.Y.Z-rcN` for release candidates, for example `v0.1.0-rc1`.
- Use `vX.Y.Z` for the final general-availability release.
- Confirm the version has not already been used as a Go submodule tag:
  `bindings/go/v0.1.0`.
- Confirm the Go binding still targets the correct module path in `bindings/go/go.mod`.
- Confirm the embedded-library filenames expected by the Go package still match the Makefile output:
  - `libpaimon_c.linux.amd64.so.zst`
  - `libpaimon_c.linux.arm64.so.zst`
  - `libpaimon_c.darwin.amd64.dylib.zst`
  - `libpaimon_c.darwin.arm64.dylib.zst`
- Confirm `python3 scripts/release_licenses.py --check` can reproduce all binary
  legal files from the locked dependency graph.
- Confirm CI is green on `main` before publishing.

## Publish

- For a coordinated Rust release, pushing the root `vX.Y.Z-rcN` or `vX.Y.Z`
  tag starts `Release Go Binding` automatically with that exact source commit.
- For a manual recovery or standalone Go release, open GitHub Actions and run
  `Release Go Binding`. Provide the release version, for example `v0.1.0`, and
  `source_ref`, for example `main`, `v0.1.0-rc1`, or a specific commit SHA.
- For release candidates, publish `bindings/go/vX.Y.Z-rcN` first and verify it before the final tag.
- Make sure the workflow is started from the intended Rust release baseline
  branch or commit lineage.
- Wait for all matrix builds to finish successfully:
  - `linux/amd64`
  - `linux/arm64`
  - `darwin/amd64`
  - `darwin/arm64`
- Confirm each build job generates and verifies the target-specific legal report.
- Confirm the workflow creates the annotated tag `bindings/go/v0.1.0`.
- Confirm the workflow publishes a GitHub release for that tag, marks an RC
  release as a pre-release, and does not replace the root project's latest release.

## After Release

- Confirm the release contains LICENSE, NOTICE, four compressed shared libraries,
  and four matching third-party license reports.
- Confirm the tag resolves to a commit that contains:
  - `bindings/go/go.mod`
  - `bindings/go/RELEASE.md`
  - the four `libpaimon_c.*.zst` files
  - `bindings/go/LICENSE` and `bindings/go/NOTICE`
  - the four `bindings/go/THIRD-PARTY-LICENSES.*.html` files
- Record which Rust release tag or baseline commit this Go binding release was
  built from.
- Verify module resolution from a clean environment:

```bash
go list -m github.com/apache/paimon-rust/bindings/go@v0.1.0
```

- Verify install/import from a small test module:

```bash
go get github.com/apache/paimon-rust/bindings/go@v0.1.0
```

```go
import paimon "github.com/apache/paimon-rust/bindings/go"
```

- If module proxy indexing is slow, retry after a few minutes and verify again.

## Rollback Notes

- Do not reuse a broken Go module version once it has been observed externally.
- Do not delete and recreate an existing `bindings/go/vX.Y.Z-rcN` or `bindings/go/vX.Y.Z` tag.
- Publish a new patch version instead, for example move from `v0.1.0` to `v0.1.1`.
- If an RC is bad, publish the next RC, for example move from `v0.1.0-rc1` to `v0.1.0-rc2`.
- If the workflow fails before pushing the tag, fix the issue and rerun with the same intended version.
- If the workflow fails after pushing the tag, rerun it with the same version and
  original source commit SHA or immutable tag. It verifies that the existing tag
  contains only the expected release artifacts on top of that source, restores
  the tagged artifacts, and resumes GitHub Release creation.
