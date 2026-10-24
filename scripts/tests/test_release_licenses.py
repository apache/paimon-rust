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

import tempfile
import unittest
from pathlib import Path

from scripts.release_licenses import (
    LicenseCorrection,
    PYTHON_MIT_CORRECTIONS,
    Report,
    binary_notice,
    bundled_component_html,
    correction_html,
    resolved_packages,
)


class LicenseCorrectionsTest(unittest.TestCase):
    def test_python_corrections_cover_jieba_workspace(self) -> None:
        corrections = [
            correction
            for correction in PYTHON_MIT_CORRECTIONS
            if correction.crates == ("jieba-macros", "jieba-rs")
        ]
        self.assertEqual(len(corrections), 1)
        correction = corrections[0]
        self.assertEqual(correction.license_crate, "jieba-rs")
        self.assertEqual(
            correction.license_path,
            "third-party-licenses/jieba-rs-0.10.3.LICENSE",
        )
        root = Path(__file__).resolve().parents[2]
        self.assertTrue((root / correction.license_path).is_file())

    def test_repository_license_correction_reads_checked_in_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            license_path = root / "third-party-licenses" / "jieba.LICENSE"
            license_path.parent.mkdir()
            license_path.write_text("Jieba workspace license\n", encoding="utf-8")
            metadata = {
                "normal_packages": {
                    ("jieba-macros", "0.10.3"),
                    ("jieba-rs", "0.10.3"),
                },
                "packages": [
                    {
                        "name": name,
                        "version": "0.10.3",
                        "manifest_path": str(root / name / "Cargo.toml"),
                        "repository": "https://example.com/jieba-rs",
                    }
                    for name in ("jieba-macros", "jieba-rs")
                ],
            }
            correction = LicenseCorrection(
                crates=("jieba-macros", "jieba-rs"),
                license_crate="jieba-rs",
                license_path="third-party-licenses/jieba.LICENSE",
                license_name="MIT License (jieba-rs workspace)",
                anchor="mit-jieba-rs-workspace",
                license_from_repository=True,
                expected_versions=(
                    ("jieba-macros", "0.10.3"),
                    ("jieba-rs", "0.10.3"),
                ),
            )

            result = correction_html(root, metadata, correction)

            self.assertIn("Jieba workspace license", result)
            self.assertIn("jieba-macros 0.10.3", result)
            self.assertIn("jieba-rs 0.10.3", result)

    def test_repository_license_correction_rejects_unverified_version(self) -> None:
        root = Path(__file__).resolve().parents[2]
        correction = next(
            correction
            for correction in PYTHON_MIT_CORRECTIONS
            if correction.crates == ("jieba-macros", "jieba-rs")
        )
        metadata = {
            "normal_packages": {
                ("jieba-macros", "9.9.9"),
                ("jieba-rs", "9.9.9"),
            },
            "packages": [
                {
                    "name": name,
                    "version": "9.9.9",
                    "manifest_path": str(root / name / "Cargo.toml"),
                    "repository": "https://example.com/jieba-rs",
                }
                for name in ("jieba-macros", "jieba-rs")
            ],
        }

        with self.assertRaisesRegex(
            RuntimeError,
            r"expected jieba-macros 0\.10\.3, found 9\.9\.9",
        ):
            correction_html(root, metadata, correction)

    def test_correction_supports_multiple_versions_with_same_license(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            packages = []
            normal_packages = set()
            for version in ("0.7.0", "0.11.0"):
                package_dir = root / f"tantivy-common-{version}"
                package_dir.mkdir()
                manifest = package_dir / "Cargo.toml"
                manifest.write_text("[package]\n", encoding="utf-8")
                (package_dir / "LICENSE").write_text(
                    "Shared Tantivy license\n",
                    encoding="utf-8",
                )
                packages.append(
                    {
                        "name": "tantivy-common",
                        "version": version,
                        "manifest_path": str(manifest),
                        "repository": "https://example.com/tantivy",
                    }
                )
                normal_packages.add(("tantivy-common", version))
            metadata = {
                "normal_packages": normal_packages,
                "packages": packages,
            }
            correction = LicenseCorrection(
                crates=("tantivy-common",),
                license_crate="tantivy-common",
                license_path="LICENSE",
                license_name="MIT License (Tantivy workspace)",
                anchor="mit-tantivy-workspace",
            )

            result = correction_html(root, metadata, correction)

            self.assertIn("tantivy-common 0.7.0", result)
            self.assertIn("tantivy-common 0.11.0", result)
            self.assertEqual(result.count("Shared Tantivy license"), 1)

    def test_jieba_license_source_is_documented_and_header_exempt(self) -> None:
        root = Path(__file__).resolve().parents[2]
        license_path = "third-party-licenses/jieba-rs-0.10.3.LICENSE"
        license_config = (root / ".licenserc.yaml").read_text(encoding="utf-8")
        third_party_readme = (
            root / "third-party-licenses" / "README.md"
        ).read_text(encoding="utf-8")

        self.assertIn(f'"{license_path}"', license_config)
        self.assertIn(
            "messense/jieba-rs/blob/"
            "c62e0df1f9dcc2cc1e014711c5aa4561ae260538/LICENSE",
            third_party_readme,
        )


class ResolvedPackagesTest(unittest.TestCase):
    def test_uses_normal_cargo_tree_packages(self) -> None:
        metadata = {
            "normal_packages": {("root", "1.0.0"), ("required", "2.0.0")},
            "packages": [
                {"name": "root", "version": "1.0.0"},
                {"name": "required", "version": "2.0.0"},
                {"name": "optional", "version": "3.0.0"},
            ],
        }
        self.assertEqual(
            [package["name"] for package in resolved_packages(metadata)],
            ["root", "required"],
        )

    def test_notice_uses_only_normal_dependencies(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "NOTICE").write_text("Root notice\n", encoding="utf-8")
            packages = []
            for name, notice in (
                ("required", "Required dependency notice\n"),
                ("optional", "Optional dependency notice\n"),
            ):
                package_dir = root / name
                package_dir.mkdir()
                manifest = package_dir / "Cargo.toml"
                manifest.write_text("[package]\n", encoding="utf-8")
                (package_dir / "NOTICE").write_text(notice, encoding="utf-8")
                packages.append(
                    {
                        "name": name,
                        "version": "1.0.0",
                        "manifest_path": str(manifest),
                    }
                )

            metadata = {
                "normal_packages": {("required", "1.0.0")},
                "packages": packages,
            }
            notice = binary_notice(root, [metadata])
            self.assertIn("Required dependency notice", notice)
            self.assertNotIn("Optional dependency notice", notice)


class ManifestCodecLicenseTest(unittest.TestCase):
    def test_python_and_go_reports_include_statically_linked_xz_license(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            crate_dir = root / "liblzma-sys"
            license_path = crate_dir / "xz" / "COPYING.0BSD"
            license_path.parent.mkdir(parents=True)
            license_path.write_text("XZ Utils bundled license\n", encoding="utf-8")
            package = {
                "name": "liblzma-sys",
                "id": "liblzma-sys-package",
                "version": "0.4.7",
                "manifest_path": str(crate_dir / "Cargo.toml"),
            }
            metadata = {
                "normal_packages": {("liblzma-sys", "0.4.7")},
                "packages": [package],
                "resolve": {
                    "nodes": [{"id": package["id"], "features": ["static"]}],
                },
            }
            report_text = (
                '<ul><li><a href="https://crates.io/crates/liblzma-sys">'
                'liblzma-sys 0.4.7</a></li></ul>'
            )
            for component in ("python", "go"):
                with self.subTest(component=component):
                    report = Report(component, "", "aarch64-apple-darwin", "")
                    result = bundled_component_html(root, report, report_text, metadata)
                    self.assertIn("XZ Utils bundled license", result)
                    self.assertIn("statically linked through", result)
                    metadata["resolve"]["nodes"][0]["features"] = []
                    with self.assertRaisesRegex(RuntimeError, "missing required features"):
                        bundled_component_html(root, report, report_text, metadata)
                    metadata["resolve"]["nodes"][0]["features"] = ["static"]


if __name__ == "__main__":
    unittest.main()
