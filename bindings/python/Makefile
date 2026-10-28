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

build:
	uv run --no-project maturin develop --locked

install:
	uv sync --group dev --no-install-project
	$(MAKE) build

test:
	uv run --no-sync pytest

test-rust:
	PYO3_PYTHON="$$(uv run --no-sync python -c 'import sys; print(sys.executable)')" \
	PYTHONHOME="$$(uv run --no-sync python -c 'import sys; print(sys.base_prefix)')" \
	PYTHONPATH="$$(uv run --no-sync python -c 'import sysconfig; print(sysconfig.get_path("purelib"))')" \
	LD_LIBRARY_PATH="$$(uv run --no-sync python -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))')$${LD_LIBRARY_PATH:+:$$LD_LIBRARY_PATH}" \
	uv run --no-sync cargo test --locked -p pypaimon_rust --lib
