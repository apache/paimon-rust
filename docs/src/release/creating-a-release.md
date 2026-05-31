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

# Creating a Release

This guide describes how to create a release of Apache Paimon Rust, including the Rust crates, Python binding, and Go binding. It follows the [ASF Release Policy](https://www.apache.org/legal/release-policy.html) and [Release Distribution Policy](https://infra.apache.org/release-distribution.html).

## Overview

![Release process overview](img/release-guide.png)

The release process consists of:

1. [Decide to release](#decide-to-release)
2. [Prepare for the release](#prepare-for-the-release)
3. [Build a release candidate](#build-a-release-candidate)
4. [Vote on the release candidate](#vote-on-the-release-candidate)
5. [If necessary, fix any issues and go back to step 3](#fix-any-issues)
6. [Finalize the release](#finalize-the-release)
7. [Promote the release](#promote-the-release)

### Automated Publishing

When a version tag is pushed, GitHub Actions automatically publishes the language-specific artifacts:

| Component        | Tag Pattern              | Published To      | Pre-release (`-rc`) Behavior          |
|------------------|--------------------------|-------------------|---------------------------------------|
| Rust crates      | `v0.1.0`                 | crates.io         | Dry-run only                          |
| Python binding   | `v0.1.0`                 | PyPI              | Publishes to TestPyPI                 |
| Go binding       | `v0.1.0`                 | Go module proxy   | Publishes as `bindings/go/vX.Y.Z-rcN` |

The Release Manager's primary responsibility is managing the **source release** (tarball + signature) and coordinating the community vote. Language artifact publishing is handled by CI once the final tag is pushed.

## Decide to Release

Deciding to release and selecting a Release Manager is the first step. This is a consensus-based decision of the community.

Anybody can propose a release on the dev [mailing list](https://lists.apache.org/list.html?dev@paimon.apache.org), giving a short rationale and nominating a committer as Release Manager (including themselves).

**Checklist**

- [ ] Community agrees to release
- [ ] A Release Manager is selected

## Prepare for the Release

### One-time Release Manager Setup

Before your first release, complete the following setup.

#### GPG Key

1. Install GnuPG if not already available:

    ```bash
    # macOS
    brew install gnupg

    # Ubuntu / Debian
    sudo apt install gnupg2
    ```

2. Generate a key pair:

    ```bash
    gpg --full-gen-key
    ```

    When prompted, select:

    - Key type: **RSA and RSA** (option 1)
    - Key size: **4096**
    - Validity: **0** (does not expire)
    - Real name and email: use your **Apache name and `@apache.org` email**

3. List your keys to find your key ID (the 8-digit hex string in the `pub` line):

    ```bash
    gpg --list-keys --keyid-format short
    ```

    Example output:

    ```text
    pub   rsa4096/845E6689 2024-01-01 [SC]
          ABCDEF1234567890ABCDEF1234567890845E6689
    uid         [ultimate] Your Name <yourname@apache.org>
    sub   rsa4096/12345678 2024-01-01 [E]
    ```

    In this example, the key ID is `845E6689`. Replace `<YOUR_KEY_ID>` with your actual key ID in the following steps.

4. Upload your public key to the Ubuntu key server:

    ```bash
    gpg --keyserver hkps://keyserver.ubuntu.com --send-keys <YOUR_KEY_ID>
    ```

5. Append your key to the project [KEYS](https://downloads.apache.org/paimon/KEYS) file (requires PMC write access):

    ```bash
    svn co https://dist.apache.org/repos/dist/release/paimon/ paimon-dist-release --depth=files
    cd paimon-dist-release
    (gpg --list-sigs <YOUR_KEY_ID> && gpg --armor --export <YOUR_KEY_ID>) >> KEYS
    svn ci -m "Add <YOUR_NAME>'s public key"
    ```

    !!! note
        Never remove existing keys from the KEYS file — users may need them to verify older releases.

6. Configure Git to sign tags with your key:

    ```bash
    git config --global user.signingkey <YOUR_KEY_ID>
    ```

    Omit `--global` to only configure signing for the current repository.

7. (Optional) Upload the GPG public key to your GitHub account:

    Go to [https://github.com/settings/keys](https://github.com/settings/keys) and add your GPG key. Make sure the email associated with the key is also added at [https://github.com/settings/emails](https://github.com/settings/emails), otherwise signed commits and tags may show as "Unverified".

#### GitHub Actions Secrets

Ensure the following repository secrets are configured:

- `CARGO_REGISTRY_TOKEN` — for crates.io publishing
- `PYPI_API_TOKEN` — for PyPI publishing
- `TEST_PYPI_API_TOKEN` — for TestPyPI publishing

### Clone into a fresh workspace

Use a clean clone to avoid local changes affecting the release.

```bash
git clone https://github.com/apache/paimon-rust.git
cd paimon-rust
```

### Set up environment variables

```bash
RELEASE_VERSION="0.1.0"
SHORT_RELEASE_VERSION="0.1"
NEXT_VERSION="0.2.0"
RELEASE_TAG="v${RELEASE_VERSION}"
RC_NUM="1"
RC_TAG="v${RELEASE_VERSION}-rc${RC_NUM}"
```

### Generate dependencies list

[ASF release policy](https://www.apache.org/legal/release-policy.html) requires that every release comply with [ASF licensing policy](https://www.apache.org/legal/resolved.html). Generate and commit a dependency list on `main` **before** creating the release branch, so both `main` and the release branch have the same list.

1. Install [cargo-deny](https://embarkstudios.github.io/cargo-deny/):

    ```bash
    cargo install cargo-deny
    ```

2. Generate the dependency list (requires **Python 3.11+**):

    ```bash
    git checkout main
    git pull
    python3 scripts/dependencies.py generate
    ```

    This creates a `DEPENDENCIES.rust.tsv` file for the workspace root and each member crate.

3. Commit the result:

    ```bash
    git add **/DEPENDENCIES*.tsv
    git commit -m "chore: update dependency list for release ${RELEASE_VERSION}"
    git push origin main
    ```

To only check licenses without generating files: `python3 scripts/dependencies.py check`.

### Create a release branch

From `main`, create a release branch:

```bash
git checkout -b release-${SHORT_RELEASE_VERSION}
git push origin release-${SHORT_RELEASE_VERSION}
```

### Bump version on main for next development cycle

After cutting the release branch, bump `main` to the next version so that ongoing development does not use the released version number:

```bash
git checkout main
./scripts/bump-version.sh ${RELEASE_VERSION} ${NEXT_VERSION}
git add Cargo.toml
git commit -m "chore: bump version to ${NEXT_VERSION}"
git push origin main
```

The script updates `version` in root `Cargo.toml` (`[workspace.package]` and the `paimon` entry in `[workspace.dependencies]`). All member crates inherit the workspace version.

### Optional: Create PRs for release blog and download page

If the project website has a release blog or download page, create pull requests to add the new version. **Do not merge these PRs until the release is finalized.**

## Build a Release Candidate

### Create the RC tag

Check out the release branch, create a signed RC tag, and push it. Pushing the tag triggers CI workflows for all three components.

```bash
git checkout release-${SHORT_RELEASE_VERSION}
git pull
git tag -s ${RC_TAG} -m "${RC_TAG}"
git push origin ${RC_TAG}
```

After pushing, verify in [GitHub Actions](https://github.com/apache/paimon-rust/actions) that all release workflows succeed:

- **Release Rust** — dry-run check for RC tags
- **Release Python Binding** — publishes to TestPyPI
- **Release Go Binding** — builds embedded libraries for all platforms, creates `bindings/go/${RC_TAG}` tag

### Create source release artifacts

From the repository root (on the release branch, at the commit you tagged):

```bash
./scripts/release.sh ${RELEASE_VERSION}
```

This creates the following under `dist/`:

- `paimon-rust-${RELEASE_VERSION}.tar.gz` — source archive
- `paimon-rust-${RELEASE_VERSION}.tar.gz.asc` — GPG signature
- `paimon-rust-${RELEASE_VERSION}.tar.gz.sha512` — SHA-512 checksum

The script automatically generates the archive from `HEAD` via `git archive`, signs it with your GPG key, and verifies the signature.

### Stage artifacts to SVN

Upload the source release to the ASF dev area:

```bash
svn checkout https://dist.apache.org/repos/dist/dev/paimon/ paimon-dist-dev --depth=immediates
cd paimon-dist-dev
mkdir paimon-rust-${RELEASE_VERSION}-rc${RC_NUM}
cp ../paimon-rust-${RELEASE_VERSION}.tar.gz* paimon-rust-${RELEASE_VERSION}-rc${RC_NUM}/
svn add paimon-rust-${RELEASE_VERSION}-rc${RC_NUM}
svn commit -m "Add paimon-rust ${RELEASE_VERSION} RC${RC_NUM}"
```

**Checklist**

- [ ] RC tag pushed and CI workflows succeeded
- [ ] Source tarball, signature, and checksum staged to [dist.apache.org dev](https://dist.apache.org/repos/dist/dev/paimon/)

## Vote on the Release Candidate

Start a vote on the dev mailing list.

**Subject:** `[VOTE] Release Apache Paimon Rust ${RELEASE_VERSION} (RC${RC_NUM})`

**Body:**

```text
Hi everyone,

Please review and vote on release candidate #${RC_NUM} for Apache Paimon Rust ${RELEASE_VERSION}.

[ ] +1 Approve the release
[ ] +0 No opinion
[ ] -1 Do not approve (please provide specific comments)

The release candidate is available at:
https://dist.apache.org/repos/dist/dev/paimon/paimon-rust-${RELEASE_VERSION}-rc${RC_NUM}/

Git tag:
https://github.com/apache/paimon-rust/releases/tag/${RC_TAG}

KEYS for signature verification:
https://downloads.apache.org/paimon/KEYS

The vote will be open for at least 72 hours.

Thanks,
Release Manager
```

After the vote passes, send a result email:

**Subject:** `[RESULT][VOTE] Release Apache Paimon Rust ${RELEASE_VERSION} (RC${RC_NUM})`

## Fix Any Issues

If the vote reveals issues:

1. Fix them on the release branch via normal PRs.
2. Remove the old RC from dist dev (optional):

```bash
cd paimon-dist-dev
svn remove paimon-rust-${RELEASE_VERSION}-rc${RC_NUM}
svn commit -m "Remove paimon-rust ${RELEASE_VERSION} RC${RC_NUM} (superseded)"
```

3. Increment `RC_NUM`, then go back to [Build a release candidate](#build-a-release-candidate).

## Finalize the Release

### Push the release tag

Once the vote passes, create and push the final release tag. This triggers CI to publish to crates.io, PyPI, and Go module proxy automatically.

```bash
git checkout ${RC_TAG}
git tag -s ${RELEASE_TAG} -m "Release Apache Paimon Rust ${RELEASE_VERSION}"
git push origin ${RELEASE_TAG}
```

### Move source artifacts to the release repository

```bash
svn mv -m "Release paimon-rust ${RELEASE_VERSION}" \
  https://dist.apache.org/repos/dist/dev/paimon/paimon-rust-${RELEASE_VERSION}-rc${RC_NUM} \
  https://dist.apache.org/repos/dist/release/paimon/paimon-rust-${RELEASE_VERSION}
```

### Verify published artifacts

- **Rust:** [crates.io/crates/paimon](https://crates.io/crates/paimon) shows version `${RELEASE_VERSION}`
- **Python:** [PyPI — pypaimon-rust](https://pypi.org/project/pypaimon-rust/) shows version `${RELEASE_VERSION}`
- **Go:** `go list -m github.com/apache/paimon-rust/bindings/go@v${RELEASE_VERSION}` resolves

### Create GitHub Release

1. Go to [Releases — New release](https://github.com/apache/paimon-rust/releases/new).
2. Choose tag `${RELEASE_TAG}`.
3. Click **Generate release notes** and review.
4. Click **Publish release**.

**Checklist**

- [ ] Release tag pushed; CI published to crates.io, PyPI, and Go module proxy
- [ ] Source artifacts moved to [dist release](https://dist.apache.org/repos/dist/release/paimon/)
- [ ] GitHub Release created

## Promote the Release

### Update the Releases page

Update the [Releases](../releases.md) page: move the released version from "Upcoming" to "Past Releases" with a summary of key features and a link to the GitHub release notes.

### Announce the release

Wait at least 24 hours after finalizing. Send the announcement to `dev@paimon.apache.org` and `announce@apache.org` using your `@apache.org` email in **plain text**.

**Subject:** `[ANNOUNCE] Release Apache Paimon Rust ${RELEASE_VERSION}`

**Body:**

```text
The Apache Paimon community is pleased to announce the release of
Apache Paimon Rust ${RELEASE_VERSION}.

Rust:   cargo add paimon
Python: pip install pypaimon-rust
Go:     go get github.com/apache/paimon-rust/bindings/go@v${RELEASE_VERSION}

Release notes:
https://github.com/apache/paimon-rust/releases/tag/v${RELEASE_VERSION}

Thanks to all contributors!
```
