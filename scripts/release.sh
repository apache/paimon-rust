#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Create ASF source release artifacts under dist/:
#   paimon-rust-{version}.tar.gz
#   paimon-rust-{version}.tar.gz.asc
#   paimon-rust-{version}.tar.gz.sha512
#
# Run from repo root. Check out the release tag first (e.g. git checkout v0.1.0-rc1).
# Usage: ./scripts/release.sh [version]
#   If version is omitted, it is read from Cargo.toml at HEAD (workspace.package.version).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [ "$#" -gt 1 ]; then
  echo "Usage: $0 [version]"
  exit 1
fi

WORKSPACE_VERSION=$(
  git show HEAD:Cargo.toml |
    awk '
      /^\[workspace\.package\]$/ { in_workspace_package = 1; next }
      in_workspace_package && /^\[/ { exit }
      in_workspace_package && /^version[[:space:]]*=/ {
        if (match($0, /"[^"]+"/)) {
          print substr($0, RSTART + 1, RLENGTH - 2)
          exit
        }
      }
    '
)
if [ -z "$WORKSPACE_VERSION" ]; then
  echo "Could not read workspace version from Cargo.toml at HEAD"
  exit 1
fi

VERSION="${1:-$WORKSPACE_VERSION}"
if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Invalid release version: ${VERSION}; expected X.Y.Z"
  exit 1
fi
if [ "$VERSION" != "$WORKSPACE_VERSION" ]; then
  echo "Release version ${VERSION} does not match workspace ${WORKSPACE_VERSION} at HEAD"
  exit 1
fi

VERSION_PATTERN=${VERSION//./\\.}
RELEASE_TAG=$(
  git tag --points-at HEAD |
    grep -E "^v${VERSION_PATTERN}(-rc[1-9][0-9]*)?$" |
    head -1 || true
)
if [ -z "$RELEASE_TAG" ]; then
  echo "HEAD must have an exact v${VERSION} or v${VERSION}-rcN release tag"
  exit 1
fi

echo "Verifying signed release tag: ${RELEASE_TAG}"
git tag -v "$RELEASE_TAG"

PREFIX="paimon-rust-${VERSION}"
DIST_DIR="${REPO_ROOT}/dist"
TARBALL="${PREFIX}.tar.gz"

echo "Creating ASF source release for paimon-rust ${VERSION}"
mkdir -p "$DIST_DIR"

echo "Creating source archive: ${TARBALL}"
git archive --format=tar.gz --prefix="${PREFIX}/" -o "${DIST_DIR}/${TARBALL}" HEAD

echo "Generating SHA-512 checksum: ${TARBALL}.sha512"
if command -v shasum >/dev/null 2>&1; then
  (cd "$DIST_DIR" && shasum -a 512 "$TARBALL" > "${TARBALL}.sha512")
else
  (cd "$DIST_DIR" && sha512sum "$TARBALL" > "${TARBALL}.sha512")
fi

echo "Signing with GPG: ${TARBALL}.asc"
SIGNING_KEY=$(git config --get user.signingkey || true)
if [ -n "$SIGNING_KEY" ]; then
  (cd "$DIST_DIR" && gpg --local-user "$SIGNING_KEY" --armor --detach-sig "$TARBALL")
else
  (cd "$DIST_DIR" && gpg --armor --detach-sig "$TARBALL")
fi

echo "Verifying signature"
(cd "$DIST_DIR" && gpg --verify "${TARBALL}.asc" "$TARBALL")

echo "Done. Artifacts in dist/:"
ls -la "${DIST_DIR}/"
echo ""
echo "Next: upload contents of dist/ to SVN (see docs/src/release/creating-a-release.md)."
