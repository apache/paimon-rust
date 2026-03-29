<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Contributing

Apache Paimon Rust welcomes contributions from everyone. See the full [Contributing Guide](https://github.com/apache/paimon-rust/blob/main/CONTRIBUTING.md) for detailed instructions.

## Quick Start

1. Fork the [repository](https://github.com/apache/paimon-rust)
2. Clone your fork: `git clone https://github.com/<your-username>/paimon-rust.git`
3. Create a feature branch: `git checkout -b feature/my-feature`
4. Make your changes and add tests
5. Run checks locally before submitting
6. Open a Pull Request

## Development Setup

```bash
# Ensure you have the correct Rust toolchain
rustup show

# Build the project
cargo build

# Run all tests
cargo test

# Format code
cargo fmt

# Lint (matches CI)
cargo clippy --all-targets --workspace -- -D warnings
```

## Finding Issues

- Check [open issues](https://github.com/apache/paimon-rust/issues) for tasks to work on
- Issues labeled `good first issue` are great starting points
- See the [0.1.0 tracking issue](https://github.com/apache/paimon-rust/issues/3) for the current roadmap

## Community

- **GitHub Issues**: [apache/paimon-rust/issues](https://github.com/apache/paimon-rust/issues)
- **Mailing List**: [dev@paimon.apache.org](mailto:dev@paimon.apache.org) ([subscribe](mailto:dev-subscribe@paimon.apache.org) / [archives](https://lists.apache.org/list.html?dev@paimon.apache.org))
- **Slack**: [#paimon channel](https://join.slack.com/t/the-asf/shared_invite/zt-2l9rns8pz-H8PE2Xnz6KraVd2Ap40z4g) on the ASF Slack
