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

# Contributing

## Get Started
This is a Rust project, so [rustup](https://rustup.rs/) is a great place to start. It provides an easy way to manage your Rust installation and toolchains.

This is a pure Rust project, so only `cargo` is needed. Here are some common commands to get you started:
- `cargo check`: Analyze the current package and report errors. This is a quick way to catch any obvious issues without a full compilation.
- `cargo fmt`: Format the current code according to the Rust style guidelines. This helps maintain a consistent code style throughout the project.
- `cargo build`: Compile the current package. This will build the project and generate executable binaries if applicable.
- `cargo clippy`: Catch common mistakes and improve code quality. Clippy provides a set of lints that can help you write better Rust code.
- `cargo test`: Run unit tests. This will execute all the tests defined in the project to ensure the functionality is correct.
- `cargo bench`: Run benchmark tests. This is useful for measuring the performance of specific parts of the code.

### Setting up the Development Environment
1. Install Rust using `rustup`. Follow the instructions on the [rustup website](https://rustup.rs/) to install Rust on your system.
2. Clone the repository to your local machine.
3. Navigate to the project directory.

### Making Changes
1. Create a new branch for your changes. This helps keep your work separate from the main development branch and makes it easier to review and merge your changes.
2. Make your changes and ensure that the code still compiles and passes all tests. Use the commands mentioned above to check for errors and run tests.
3. Format your code using `cargo fmt` to ensure consistency with the project's code style.

### Submitting Changes
1. Once you are satisfied with your changes, push your branch to the remote repository.
2. Open a pull request on the project's GitHub page. Provide a clear description of your changes and why they are necessary.
3. Wait for reviews and address any feedback. Once the pull request is approved and merged, your changes will be part of the project.

### Read the design docs 
For a deeper understanding of the project, read the design documentation available on our [Paimon official website](https://paimon.apache.org/).

Thank you for contributing to this project! 😊
