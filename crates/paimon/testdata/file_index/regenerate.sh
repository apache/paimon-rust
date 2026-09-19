#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <paimon-java-checkout> <empty-output-directory>" >&2
  exit 2
fi

readonly EXPECTED_COMMIT="1d368b4a5932f8221fd28e2555001abdb8fb12ee"
readonly JAVA_CHECKOUT="$1"
readonly OUTPUT="$2"
readonly SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
readonly SOURCE="$SCRIPT_DIR/java/JavaFileIndexTableFixtureGeneratorTest.java"
readonly TARGET="$JAVA_CHECKOUT/paimon-core/src/test/java/org/apache/paimon/fixture/JavaFileIndexTableFixtureGeneratorTest.java"

actual_commit="$(git -C "$JAVA_CHECKOUT" rev-parse HEAD)"
if [[ "$actual_commit" != "$EXPECTED_COMMIT" ]]; then
  echo "expected Apache Paimon Java $EXPECTED_COMMIT, got $actual_commit" >&2
  exit 1
fi
if [[ -e "$TARGET" ]]; then
  echo "refusing to overwrite $TARGET" >&2
  exit 1
fi
if [[ -e "$OUTPUT" ]] && [[ -n "$(find "$OUTPUT" -mindepth 1 -print -quit)" ]]; then
  echo "output directory must be empty: $OUTPUT" >&2
  exit 1
fi

mkdir -p "$(dirname "$TARGET")" "$OUTPUT"
cp "$SOURCE" "$TARGET"
trap 'rm -f "$TARGET"' EXIT

mvn -f "$JAVA_CHECKOUT/pom.xml" \
  -pl paimon-codegen-loader -am -Pfast-build \
  -DskipTests \
  package

mvn -f "$JAVA_CHECKOUT/pom.xml" \
  -pl paimon-core -am -Pfast-build \
  -DfailIfNoTests=false \
  -DwildcardSuites=none \
  -Dtest=JavaFileIndexTableFixtureGeneratorTest \
  -Dgenerate.file-index.fixture.output="$OUTPUT" \
  test

echo "generated fixtures under $OUTPUT/default.db"
