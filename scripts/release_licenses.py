#!/usr/bin/env python3

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generate artifact-exact legal metadata for native release artifacts."""

from __future__ import annotations

import argparse
import html
import json
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path


CARGO_ABOUT_VERSION = "0.9.1"
PYTHON_TARGETS = (
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
    "x86_64-apple-darwin",
    "aarch64-apple-darwin",
    "x86_64-pc-windows-msvc",
)
GO_REPORTS = {
    "x86_64-unknown-linux-gnu": "THIRD-PARTY-LICENSES.linux.amd64.html",
    "aarch64-unknown-linux-gnu": "THIRD-PARTY-LICENSES.linux.arm64.html",
    "x86_64-apple-darwin": "THIRD-PARTY-LICENSES.darwin.amd64.html",
    "aarch64-apple-darwin": "THIRD-PARTY-LICENSES.darwin.arm64.html",
}


@dataclass(frozen=True)
class Report:
    component: str
    manifest: str
    target: str
    output: str


@dataclass(frozen=True)
class LicenseCorrection:
    crates: tuple[str, ...]
    license_crate: str
    license_path: str
    license_name: str
    anchor: str
    license_from_repository: bool = False
    expected_versions: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class BundledComponent:
    crate: str
    license_path: str
    component: str
    component_url: str
    license_name: str
    anchor: str
    components: tuple[str, ...] = ()
    targets: tuple[str, ...] = ()
    license_from_repository: bool = False
    required: bool = False
    relationship: str = "bundled by"
    crate_version: str = ""
    required_features: tuple[str, ...] = ()


ALLOC_CORRECTIONS = (
    LicenseCorrection(
        crates=("alloc-stdlib",),
        license_crate="alloc-no-stdlib",
        license_path="LICENSE",
        license_name="BSD 3-Clause License (rust-alloc-no-stdlib)",
        anchor="bsd-rust-alloc-no-stdlib",
    ),
    LicenseCorrection(
        crates=("aws-lc-sys",),
        license_crate="aws-lc-sys",
        license_path="LICENSE",
        license_name="AWS-LC License",
        anchor="aws-lc-license",
    ),
)

COMMON_MIT_CORRECTIONS = (
    LicenseCorrection(
        crates=("async-stream", "async-stream-impl"),
        license_crate="async-stream",
        license_path="LICENSE",
        license_name="MIT License (async-stream workspace)",
        anchor="mit-async-stream-workspace",
    ),
    LicenseCorrection(
        crates=("brotli-decompressor",),
        license_crate="brotli-decompressor",
        license_path="LICENSE",
        license_name="MIT License (brotli-decompressor)",
        anchor="mit-brotli-decompressor",
    ),
    LicenseCorrection(
        crates=("libm",),
        license_crate="libm",
        license_path="LICENSE.txt",
        license_name="MIT License (libm)",
        anchor="mit-libm",
    ),
)

PYTHON_MIT_CORRECTIONS = (
    LicenseCorrection(
        crates=("jieba-macros", "jieba-rs"),
        license_crate="jieba-rs",
        license_path="third-party-licenses/jieba-rs-0.10.3.LICENSE",
        license_name="MIT License (jieba-rs workspace)",
        anchor="mit-jieba-rs-workspace",
        license_from_repository=True,
        expected_versions=(
            ("jieba-macros", "0.10.3"),
            ("jieba-rs", "0.10.3"),
        ),
    ),
    LicenseCorrection(
        crates=(
            "ownedbytes",
            "tantivy-bitpacker",
            "tantivy-columnar",
            "tantivy-common",
            "tantivy-query-grammar",
            "tantivy-sstable",
            "tantivy-stacker",
            "tantivy-tokenizer-api",
        ),
        license_crate="tantivy",
        license_path="LICENSE",
        license_name="MIT License (Tantivy workspace)",
        anchor="mit-tantivy-workspace",
    ),
)

BUNDLED_COMPONENTS = (
    BundledComponent(
        crate="zstd-sys",
        license_path="zstd/LICENSE",
        component="vendored Zstandard C sources",
        component_url="https://github.com/facebook/zstd",
        license_name="BSD 3-Clause License",
        anchor="bundled-zstandard-bsd-3-clause",
    ),
    BundledComponent(
        crate="rust-stemmers",
        license_path="algorithms/LICENSE",
        component="generated Snowball stemming algorithms",
        component_url="https://snowballstem.org/",
        license_name="BSD 3-Clause License",
        anchor="bundled-snowball-bsd-3-clause",
    ),
    BundledComponent(
        crate="regex-syntax",
        license_path="src/unicode_tables/LICENSE-UNICODE",
        component="generated Unicode character database tables",
        component_url="https://www.unicode.org/",
        license_name="Unicode Data Files and Software License",
        anchor="bundled-regex-syntax-unicode",
    ),
    BundledComponent(
        crate="liblzma-sys",
        license_path="xz/COPYING.0BSD",
        component="XZ Utils 5.8.3 liblzma C sources",
        component_url="https://github.com/tukaani-project/xz/tree/v5.8.3",
        license_name="BSD Zero Clause License",
        anchor="bundled-xz-utils-0bsd",
        components=("python", "go"),
        required=True,
        relationship="statically linked through",
        crate_version="0.4.7",
        required_features=("static",),
    ),
    BundledComponent(
        crate="openssl-sys",
        license_path="third-party-licenses/openssl-1.1.1.LICENSE",
        component="OpenSSL 1.1.1k FIPS libssl and libcrypto shared libraries",
        component_url="https://github.com/openssl/openssl/tree/OpenSSL_1_1_1k",
        license_name="OpenSSL 1.1.1 and Original SSLeay Licenses",
        anchor="bundled-openssl-1.1.1",
        components=("python",),
        targets=("x86_64-unknown-linux-gnu",),
        license_from_repository=True,
        required=True,
        relationship="linked through",
    ),
    BundledComponent(
        crate="openssl-sys",
        license_path="third-party-licenses/openssl-1.1.1.LICENSE",
        component="OpenSSL 1.1.1w libssl and libcrypto shared libraries",
        component_url="https://github.com/openssl/openssl/tree/OpenSSL_1_1_1w",
        license_name="OpenSSL 1.1.1 and Original SSLeay Licenses",
        anchor="bundled-openssl-1.1.1",
        components=("python",),
        targets=("aarch64-unknown-linux-gnu",),
        license_from_repository=True,
        required=True,
        relationship="linked through",
    ),
)

ALLOC_PLACEHOLDER = "Copyright (c) &lt;year&gt; &lt;owner&gt;."
MIT_PLACEHOLDER = "Copyright (c) &lt;year&gt; &lt;copyright holders&gt;"
LICENSE_PLACEHOLDERS = (
    "&lt;year&gt;",
    "&lt;owner&gt;",
    "&lt;copyright holders&gt;",
    "<year>",
    "<owner>",
    "<copyright holders>",
)
ASF_DEVELOPED_NOTICE = re.compile(
    r"This product includes software developed at\n"
    r"The Apache Software Foundation \(https?://www\.apache\.org/\)\."
)


def repository_root() -> Path:
    return Path(
        subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"], text=True
        ).strip()
    )


def report_specs() -> list[Report]:
    reports = [
        Report(
            component="python",
            manifest="bindings/python/Cargo.toml",
            target=target,
            output=(f"bindings/python/licenses/{target}/THIRD-PARTY-LICENSES.html"),
        )
        for target in PYTHON_TARGETS
    ]
    reports.extend(
        Report(
            component="go",
            manifest="bindings/c/Cargo.toml",
            target=target,
            output=f"bindings/go/{filename}",
        )
        for target, filename in GO_REPORTS.items()
    )
    return reports


def verify_cargo_about(root: Path) -> None:
    output = subprocess.check_output(
        ["cargo", "about", "--version"], cwd=root, text=True
    ).strip()
    actual = output.rsplit(" ", 1)[-1]
    if actual != CARGO_ABOUT_VERSION:
        raise RuntimeError(
            f"cargo-about {CARGO_ABOUT_VERSION} is required, found {output!r}"
        )


def cargo_metadata(root: Path, report: Report) -> dict:
    output = subprocess.check_output(
        [
            "cargo",
            "metadata",
            "--locked",
            "--format-version",
            "1",
            "--manifest-path",
            report.manifest,
            "--filter-platform",
            report.target,
        ],
        cwd=root,
        text=True,
    )
    metadata = json.loads(output)
    tree = subprocess.check_output(
        [
            "cargo",
            "tree",
            "--locked",
            "--manifest-path",
            report.manifest,
            "--target",
            report.target,
            "--edges",
            "normal",
            "--prefix",
            "none",
            "--format",
            "{p}",
        ],
        cwd=root,
        text=True,
    )
    packages = set()
    for line in tree.splitlines():
        match = re.match(r"^([A-Za-z0-9_-]+) v([^ ]+)", line)
        if match is None:
            raise RuntimeError(f"unexpected cargo tree package line: {line!r}")
        packages.add(match.groups())
    metadata["normal_packages"] = packages
    return metadata


def generate_base_report(root: Path, report: Report, output: Path) -> str:
    subprocess.run(
        [
            "cargo",
            "about",
            "generate",
            "--frozen",
            "--fail",
            "--config",
            str(root / "about.toml"),
            "--manifest-path",
            report.manifest,
            "--target",
            report.target,
            "--output-file",
            str(output),
            str(root / "about.hbs"),
        ],
        cwd=root,
        check=True,
    )
    return output.read_text(encoding="utf-8")


def resolved_packages(metadata: dict) -> list[dict]:
    normal_packages = metadata["normal_packages"]
    return [
        package
        for package in metadata["packages"]
        if (package["name"], package["version"]) in normal_packages
    ]


def packages_by_name(metadata: dict, crate_name: str) -> list[dict]:
    return [
        package
        for package in resolved_packages(metadata)
        if package["name"] == crate_name
    ]


def package_by_name(
    metadata: dict, crate_name: str, required: bool = True
) -> dict | None:
    matches = packages_by_name(metadata, crate_name)
    if not matches and not required:
        return None
    if len(matches) != 1:
        versions = [package["version"] for package in matches]
        raise RuntimeError(
            f"expected exactly one resolved {crate_name} package, found {versions}"
        )
    return matches[0]


def package_features(metadata: dict, package: dict) -> set[str]:
    matches = [
        node["features"]
        for node in metadata["resolve"]["nodes"]
        if node["id"] == package["id"]
    ]
    if len(matches) != 1:
        raise RuntimeError(f"could not resolve features for {package['name']}")
    return set(matches[0])


def correction_html(
    root: Path, metadata: dict, correction: LicenseCorrection
) -> str:
    expected_versions = dict(correction.expected_versions)
    used_by = []
    for crate_name in correction.crates:
        packages = packages_by_name(metadata, crate_name)
        if not packages:
            raise RuntimeError(f"expected at least one resolved {crate_name} package")
        for package in packages:
            if correction.license_from_repository:
                expected_version = expected_versions.get(crate_name)
                if expected_version is None:
                    raise RuntimeError(
                        "repository-backed correction is missing an expected "
                        f"version for {crate_name}"
                    )
                if package["version"] != expected_version:
                    raise RuntimeError(
                        f"expected {crate_name} {expected_version}, "
                        f"found {package['version']}"
                    )
            repository = package.get("repository") or (
                f"https://crates.io/crates/{crate_name}"
            )
            used_by.append(
                f'                    <li><a href="{html.escape(repository, quote=True)}">'
                f"{html.escape(crate_name)} {html.escape(package['version'])}</a></li>"
            )

    if correction.license_from_repository:
        license_file = root / correction.license_path
        if not license_file.is_file():
            raise RuntimeError(f"corrected license file is missing: {license_file}")
        license_text = license_file.read_text(encoding="utf-8")
    else:
        license_files = [
            Path(package["manifest_path"]).parent / correction.license_path
            for package in packages_by_name(metadata, correction.license_crate)
        ]
        if not license_files:
            raise RuntimeError(
                f"expected at least one resolved {correction.license_crate} package"
            )
        missing = [path for path in license_files if not path.is_file()]
        if missing:
            raise RuntimeError(f"corrected license file is missing: {missing[0]}")
        license_texts = {
            path.read_text(encoding="utf-8") for path in license_files
        }
        if len(license_texts) != 1:
            raise RuntimeError(
                f"corrected license files differ for {correction.license_crate}"
            )
        license_text = license_texts.pop()

    return "\n".join(
        [
            '            <li class="license corrected-license">',
            f'                <h3 id="{html.escape(correction.anchor)}">'
            f"{html.escape(correction.license_name)}</h3>",
            "                <h4>Used by</h4>",
            '                <ul class="license-used-by">',
            *used_by,
            "                </ul>",
            f'                <pre class="license-text">{html.escape(license_text)}</pre>',
            "            </li>",
        ]
    )


def replace_placeholder_entry(
    root: Path,
    report_text: str,
    metadata: dict,
    marker: str,
    corrections: tuple[LicenseCorrection, ...],
) -> str:
    if report_text.count(marker) != 1:
        raise RuntimeError(
            f"expected exactly one license placeholder {marker!r}, "
            f"found {report_text.count(marker)}"
        )
    marker_index = report_text.index(marker)
    entry_start = report_text.rfind('            <li class="license">', 0, marker_index)
    entry_end = report_text.find("            </li>", marker_index)
    if entry_start == -1 or entry_end == -1:
        raise RuntimeError(f"could not locate license entry for {marker!r}")
    entry_end += len("            </li>")
    placeholder_entry = report_text[entry_start:entry_end]

    actual_crates = set(
        re.findall(
            r'<li><a href="[^"]+">([A-Za-z0-9_-]+) [^<]+</a></li>',
            placeholder_entry,
        )
    )
    expected_crates = {
        crate_name for correction in corrections for crate_name in correction.crates
    }
    if actual_crates != expected_crates:
        raise RuntimeError(
            f"placeholder dependency set changed for {marker!r}: expected "
            f"{sorted(expected_crates)}, found {sorted(actual_crates)}"
        )

    replacement = "\n".join(
        correction_html(root, metadata, correction) for correction in corrections
    )
    return report_text[:entry_start] + replacement + report_text[entry_end:]


def bundled_component_html(
    root: Path, report: Report, report_text: str, metadata: dict
) -> str:
    items = []
    for component in BUNDLED_COMPONENTS:
        if component.components and report.component not in component.components:
            continue
        if component.targets and report.target not in component.targets:
            continue
        package = package_by_name(metadata, component.crate, required=False)
        if package is None:
            if component.required:
                raise RuntimeError(
                    f"required bundled component crate is missing: {component.crate}"
                )
            continue
        if component.crate_version and package["version"] != component.crate_version:
            raise RuntimeError(
                f"expected {component.crate} {component.crate_version}, "
                f"found {package['version']}"
            )
        missing_features = set(component.required_features) - package_features(
            metadata, package
        )
        if missing_features:
            raise RuntimeError(
                f"{component.crate} is missing required features: "
                f"{sorted(missing_features)}"
            )
        marker = f">{component.crate} {package['version']}</a>"
        if marker not in report_text:
            if component.required:
                raise RuntimeError(
                    f"required bundled component is absent from report: {component.crate}"
                )
            # Cargo metadata may include workspace-unified features.
            continue
        if component.license_from_repository:
            license_file = root / component.license_path
        else:
            license_file = (
                Path(package["manifest_path"]).parent / component.license_path
            )
        if not license_file.is_file():
            raise RuntimeError(f"bundled license file is missing: {license_file}")
        license_text = license_file.read_text(encoding="utf-8")
        repository = package.get("repository") or (
            f"https://crates.io/crates/{component.crate}"
        )
        items.append(
            "\n".join(
                [
                    '            <li class="license bundled-subcomponent">',
                    f'                <h3 id="{html.escape(component.anchor)}">'
                    f"{html.escape(component.license_name)}</h3>",
                    "                <h4>Bundled component</h4>",
                    '                <ul class="license-used-by">',
                    "                    <li>",
                    f'                        <a href="{html.escape(component.component_url, quote=True)}">'
                    f"{html.escape(component.component)}</a>, "
                    f"{html.escape(component.relationship)}",
                    f'                        <a href="{html.escape(repository, quote=True)}">'
                    f"{html.escape(component.crate)} {html.escape(package['version'])}</a>",
                    "                    </li>",
                    "                </ul>",
                    f'                <pre class="license-text">{html.escape(license_text)}</pre>',
                    "            </li>",
                ]
            )
        )

    if not items:
        return ""
    return "\n".join(
        [
            "",
            "        <h2>Licenses for bundled native and source components</h2>",
            "        <p>",
            "            These components ship with the native artifact but have",
            "            license files outside their Rust package metadata.",
            "        </p>",
            '        <ul class="licenses-list bundled-subcomponents">',
            *items,
            "        </ul>",
        ]
    )


def complete_report(
    root: Path, base_report: str, report: Report, metadata: dict
) -> str:
    result = replace_placeholder_entry(
        root, base_report, metadata, ALLOC_PLACEHOLDER, ALLOC_CORRECTIONS
    )
    mit_corrections = COMMON_MIT_CORRECTIONS
    if report.component == "python":
        mit_corrections += PYTHON_MIT_CORRECTIONS
    result = replace_placeholder_entry(
        root, result, metadata, MIT_PLACEHOLDER, mit_corrections
    )

    for placeholder in LICENSE_PLACEHOLDERS:
        if placeholder in result:
            raise RuntimeError(
                f"generated report still contains license placeholder {placeholder!r}"
            )

    description = (
        "\n        <p><strong>Artifact:</strong> "
        f"<code>{html.escape(report.component)}</code></p>"
        "\n        <p><strong>Rust target:</strong> "
        f"<code>{html.escape(report.target)}</code></p>"
        "\n        <p><strong>Root manifest:</strong> "
        f"<code>{html.escape(report.manifest)}</code></p>"
    )
    first_paragraph_end = result.find("</p>")
    if first_paragraph_end == -1:
        raise RuntimeError("about.hbs output has no introductory paragraph")
    first_paragraph_end += len("</p>")
    result = result[:first_paragraph_end] + description + result[first_paragraph_end:]

    closing_main = result.rfind("    </main>")
    if closing_main == -1:
        raise RuntimeError("about.hbs output has no closing main element")
    result = (
        result[:closing_main]
        + bundled_component_html(root, report, result, metadata)
        + "\n"
        + result[closing_main:]
    )
    return "\n".join(line.rstrip() for line in result.rstrip().splitlines()) + "\n"


def binary_license(apache_license: str, heading: str, reports: list[str]) -> str:
    appendix = [
        "",
        "=" * 79,
        "BUNDLED THIRD-PARTY COMPONENTS",
        "=" * 79,
        "",
        heading,
        "The component inventory, copyright notices, and complete license texts",
        "are provided in:",
        "",
    ]
    appendix.extend(f"    {report}" for report in reports)
    return apache_license.rstrip() + "\n" + "\n".join(appendix) + "\n"


def notice_paragraphs(text: str) -> list[str]:
    normalized = "\n".join(line.rstrip() for line in text.strip().splitlines())
    return [part.strip() for part in re.split(r"\n\s*\n", normalized) if part.strip()]


def binary_notice(root: Path, metadata_by_target: list[dict]) -> str:
    paragraphs = notice_paragraphs((root / "NOTICE").read_text(encoding="utf-8"))
    seen_paragraphs = set(paragraphs)
    seen_packages = set()
    for metadata in metadata_by_target:
        for package in resolved_packages(metadata):
            package_key = (
                package["name"],
                package["version"],
                package["manifest_path"],
            )
            if package_key in seen_packages:
                continue
            seen_packages.add(package_key)
            package_dir = Path(package["manifest_path"]).parent
            notice_files = sorted(
                path
                for path in package_dir.iterdir()
                if path.is_file()
                and path.name.lower() in {"notice", "notice.txt", "notice.md"}
            )
            for notice_file in notice_files:
                notice_text = notice_file.read_text(encoding="utf-8")
                for paragraph in notice_paragraphs(notice_text):
                    if ASF_DEVELOPED_NOTICE.fullmatch(paragraph):
                        continue
                    if paragraph in seen_paragraphs:
                        continue
                    seen_paragraphs.add(paragraph)
                    paragraphs.append(paragraph)

    return "\n\n".join(paragraphs) + "\n"


def verify_python_source_legal_files(root: Path) -> None:
    for name in ("LICENSE", "NOTICE"):
        source = root / name
        copy = root / "bindings/python" / name
        if source.read_bytes() != copy.read_bytes():
            raise RuntimeError(f"restore bindings/python/{name} before generation")
    source_report = root / "bindings/python/THIRD-PARTY-LICENSES.html"
    report_text = source_report.read_text(encoding="utf-8")
    for marker in (
        "Python source distribution licenses",
        "contains no compiled native library",
    ):
        if marker not in report_text:
            raise RuntimeError(f"Python source license report is missing {marker!r}")


def generate_reports(
    root: Path, reports: list[Report]
) -> tuple[dict[Report, str], dict[Report, dict]]:
    verify_cargo_about(root)
    result = {}
    metadata_by_report = {}
    with tempfile.TemporaryDirectory(prefix="paimon-rust-license-reports-") as temp:
        temp_root = Path(temp)
        for index, report in enumerate(reports):
            print(f"generating {report.component} license report for {report.target}")
            base = generate_base_report(
                root, report, temp_root / f"report-{index}.html"
            )
            metadata = cargo_metadata(root, report)
            metadata_by_report[report] = metadata
            result[report] = complete_report(root, base, report, metadata)
    return result, metadata_by_report


def python_binary_files(
    root: Path, target: str, report: str, metadata: dict
) -> dict[str, str]:
    apache_license = (root / "LICENSE").read_text(encoding="utf-8")
    return {
        "LICENSE": binary_license(
            apache_license,
            f"This binary wheel bundles the Rust native library for {target}.",
            ["THIRD-PARTY-LICENSES.html"],
        ),
        "NOTICE": binary_notice(root, [metadata]),
        "THIRD-PARTY-LICENSES.html": report,
    }


def python_target_files(root: Path, target: str, output_dir: Path) -> dict[Path, str]:
    if target not in PYTHON_TARGETS:
        raise ValueError(f"unsupported Python release target: {target}")
    verify_python_source_legal_files(root)
    report = next(
        item
        for item in report_specs()
        if item.component == "python" and item.target == target
    )
    reports, metadata = generate_reports(root, [report])
    return {
        output_dir / name: content
        for name, content in python_binary_files(
            root, target, reports[report], metadata[report]
        ).items()
    }


def python_verification_files(root: Path, output_dir: Path) -> dict[Path, str]:
    verify_python_source_legal_files(root)
    reports = [report for report in report_specs() if report.component == "python"]
    generated, metadata = generate_reports(root, reports)
    result = {}
    for report in reports:
        target_dir = output_dir / report.target
        result.update(
            {
                target_dir / name: content
                for name, content in python_binary_files(
                    root,
                    report.target,
                    generated[report],
                    metadata[report],
                ).items()
            }
        )
    return result


def go_target_files(root: Path, target: str, output_dir: Path) -> dict[Path, str]:
    if target not in GO_REPORTS:
        raise ValueError(f"unsupported Go release target: {target}")
    report = next(
        item
        for item in report_specs()
        if item.component == "go" and item.target == target
    )
    generated, _ = generate_reports(root, [report])
    return {output_dir / GO_REPORTS[target]: generated[report]}


def go_release_files(root: Path, output_dir: Path) -> dict[Path, str]:
    reports = [report for report in report_specs() if report.component == "go"]
    generated, metadata = generate_reports(root, reports)
    apache_license = (root / "LICENSE").read_text(encoding="utf-8")
    result = {
        output_dir / GO_REPORTS[report.target]: generated[report] for report in reports
    }
    result[output_dir / "LICENSE"] = binary_license(
        apache_license,
        "This Go module bundles Rust native libraries for four release targets.",
        list(GO_REPORTS.values()),
    )
    result[output_dir / "NOTICE"] = binary_notice(
        root, [metadata[report] for report in reports]
    )
    return result


def generated_files(root: Path) -> dict[Path, str]:
    verify_python_source_legal_files(root)
    reports = report_specs()
    generated, metadata = generate_reports(root, reports)
    result = {root / report.output: generated[report] for report in reports}

    apache_license = (root / "LICENSE").read_text(encoding="utf-8")
    python_reports = [report for report in reports if report.component == "python"]
    for report in python_reports:
        license_dir = root / "bindings/python/licenses" / report.target
        files = python_binary_files(
            root, report.target, generated[report], metadata[report]
        )
        for name in ("LICENSE", "NOTICE"):
            result[license_dir / name] = files[name]

    go_reports = list(GO_REPORTS.values())
    result[root / "bindings/go/LICENSE"] = binary_license(
        apache_license,
        "This Go module bundles Rust native libraries for four release targets.",
        go_reports,
    )
    result[root / "bindings/go/NOTICE"] = binary_notice(
        root,
        [
            metadata[report]
            for report in reports
            if report.component == "go"
        ],
    )
    return result


def check_generated_files(files: dict[Path, str], root: Path) -> None:
    relative_paths = [str(path.relative_to(root)) for path in files]
    tracked = subprocess.check_output(
        ["git", "ls-files", "--", *relative_paths], cwd=root, text=True
    ).splitlines()
    if tracked:
        raise RuntimeError(
            "generated binary legal files must not be tracked: "
            + ", ".join(tracked)
        )

    python_paths = [
        relative
        for relative in relative_paths
        if relative.startswith("bindings/python/licenses/")
    ]
    for relative in python_paths:
        attribute = subprocess.check_output(
            ["git", "check-attr", "export-ignore", "--", relative],
            cwd=root,
            text=True,
        ).strip()
        if not attribute.endswith(": set"):
            raise RuntimeError(f"generated binary legal file is not export-ignored: {relative}")

    print(f"validated {len(files)} generated binary legal files")


def write_files(files: dict[Path, str], root: Path) -> None:
    for path, content in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content.encode("utf-8"))
        display_path = path.relative_to(root) if path.is_relative_to(root) else path
        print(f"generated {display_path}")


def verify_existing_go_reports(files: dict[Path, str]) -> None:
    report_names = set(GO_REPORTS.values())
    for path, expected in files.items():
        if path.name not in report_names:
            continue
        if not path.is_file():
            raise RuntimeError(f"missing generated Go report: {path}")
        if path.read_bytes() != expected.encode("utf-8"):
            raise RuntimeError(f"generated Go report differs from release input: {path}")


def stage_python(root: Path, target: str) -> None:
    write_files(python_target_files(root, target, root / "bindings/python"), root)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--check",
        action="store_true",
        help="validate reproducible binary legal output without writing it",
    )
    group.add_argument(
        "--stage-python",
        metavar="TARGET",
        help="stage one target's legal files for a maturin wheel build",
    )
    group.add_argument(
        "--generate-python-reports",
        metavar="DIRECTORY",
        help="generate all target legal files for final wheel verification",
    )
    group.add_argument(
        "--generate-go-report",
        nargs=2,
        metavar=("TARGET", "DIRECTORY"),
        help="generate one target report for a Go build job",
    )
    group.add_argument(
        "--generate-go-release",
        metavar="DIRECTORY",
        help="verify Go target reports and generate aggregate legal files",
    )
    args = parser.parse_args()
    root = repository_root()

    try:
        if args.stage_python:
            stage_python(root, args.stage_python)
            return 0
        if args.generate_python_reports:
            files = python_verification_files(
                root, Path(args.generate_python_reports).resolve()
            )
            write_files(files, root)
            return 0
        if args.generate_go_report:
            target, directory = args.generate_go_report
            files = go_target_files(root, target, Path(directory).resolve())
            write_files(files, root)
            return 0
        if args.generate_go_release:
            files = go_release_files(root, Path(args.generate_go_release).resolve())
            verify_existing_go_reports(files)
            write_files(
                {
                    path: content
                    for path, content in files.items()
                    if path.name in {"LICENSE", "NOTICE"}
                },
                root,
            )
            return 0
        files = generated_files(root)
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        print(f"release license generation failed: {error}", file=sys.stderr)
        return 1

    if args.check:
        try:
            check_generated_files(files, root)
        except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
            print(f"release license generation failed: {error}", file=sys.stderr)
            return 1
        return 0
    write_files(files, root)
    return 0


if __name__ == "__main__":
    sys.exit(main())
