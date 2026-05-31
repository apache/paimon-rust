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
# Bump version in root Cargo.toml ([workspace.package] and [workspace.dependencies]).
# Run from repo root.
#
# Usage: ./scripts/bump-version.sh <current_version> <next_version>
#   e.g. ./scripts/bump-version.sh 0.1.0 0.2.0

set -e

if [ -z "$1" ] || [ -z "$2" ]; then
  echo "Usage: $0 <current_version> <next_version>"
  echo "  e.g. $0 0.1.0 0.2.0"
  exit 1
fi

FROM_VERSION="$1"
TO_VERSION="$2"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [ ! -f Cargo.toml ]; then
  echo "Cargo.toml not found. Run from repo root."
  exit 1
fi

# Replace version in [workspace.package] section.
# Also update the paimon crate version in [workspace.dependencies].
case "$(uname -s)" in
  Darwin)
    sed -i '' '/^\[workspace\.package\]/,/^\[/{s/version = "'"${FROM_VERSION}"'"/version = "'"${TO_VERSION}"'"/;}' Cargo.toml
    sed -i '' 's/^paimon = { version = "'"${FROM_VERSION}"'"/paimon = { version = "'"${TO_VERSION}"'"/' Cargo.toml
    ;;
  *)
    sed -i '/^\[workspace\.package\]/,/^\[/{s/version = "'"${FROM_VERSION}"'"/version = "'"${TO_VERSION}"'"/;}' Cargo.toml
    sed -i 's/^paimon = { version = "'"${FROM_VERSION}"'"/paimon = { version = "'"${TO_VERSION}"'"/' Cargo.toml
    ;;
esac

echo "Bumped version from ${FROM_VERSION} to ${TO_VERSION} in Cargo.toml"
echo "Review with: git diff Cargo.toml"
