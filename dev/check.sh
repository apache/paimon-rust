#!/bin/bash
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

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
AUTO_FIX=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --fix)
            AUTO_FIX=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--fix]"
            exit 1
            ;;
    esac
done

cd "$PROJECT_ROOT"

echo "========================================"
echo "Running code checks for paimon-rust"
echo "========================================"

# 1. Check license headers
echo ""
echo "[1/4] Checking license headers..."
if command -v license-eye &> /dev/null; then
    license-eye header check
    echo "✓ License check passed"
else
    echo "⚠ Failed to check license headers. license-eye not installed, skipping license check"
fi

# 2. Check code formatting (with auto-fix)
echo ""
echo "[2/4] Checking code formatting (cargo fmt)..."
if ! cargo fmt --all -- --check; then
    if [ "$AUTO_FIX" = true ]; then
        echo "Fixing formatting issues..."
        cargo fmt --all
        echo "✓ Format fixed"
    else
        echo "✗ Format check failed. Run with --fix to auto-fix, or run 'cargo fmt --all' manually."
        exit 1
    fi
else
    echo "✓ Format check passed"
fi

# 3. Run clippy (with auto-fix)
echo ""
echo "[3/4] Running clippy..."
if ! cargo clippy --all-targets --all-features --workspace 2>&1; then
    if [ "$AUTO_FIX" = true ]; then
        echo "Attempting to fix clippy issues..."
        cargo clippy --fix --allow-dirty --allow-staged --all-targets --all-features --workspace
        echo "✓ Clippy fixes applied. Re-running clippy..."
        if ! cargo clippy --all-targets --all-features --workspace; then
            echo "✗ Some clippy issues could not be auto-fixed. Please fix manually."
            exit 1
        fi
        echo "✓ Clippy check passed after fixes"
    else
        echo "✗ Clippy check failed. Run with --fix to auto-fix, or fix manually."
        exit 1
    fi
else
    echo "✓ Clippy check passed"
fi

# 4. Run tests
echo ""
echo "[4/4] Running tests..."
cargo test --workspace --lib
echo "✓ Tests passed"

echo ""
echo "========================================"
echo "All checks passed! ✓"
echo "========================================"
