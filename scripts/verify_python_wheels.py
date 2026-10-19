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

"""Verify Python release archives and their legal metadata."""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import re
import stat
import sys
import tarfile
import tomllib
import unicodedata
import zipfile
from email import policy
from email.message import Message
from email.parser import BytesParser
from pathlib import Path, PurePosixPath


ROOT = Path(__file__).resolve().parents[1]
PYTHON_DIR = ROOT / "bindings/python"
LEGAL_FILES = ("LICENSE", "NOTICE", "THIRD-PARTY-LICENSES.html")
TARGETS = {
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
    "x86_64-apple-darwin",
    "aarch64-apple-darwin",
    "x86_64-pc-windows-msvc",
}
PLACEHOLDERS = (
    b"&lt;year&gt;",
    b"&lt;owner&gt;",
    b"&lt;copyright holders&gt;",
    b"<year>",
    b"<owner>",
    b"<copyright holders>",
)
AVRO_NOTICE_MARKERS = (
    b"Apache Avro",
    b"Flavien Raynaud",
    b"donated to the Apache Avro project in 2020.",
)
XZ_LICENSE_ANCHOR = b'id="bundled-xz-utils-0bsd"'


class VerificationError(RuntimeError):
    """A release archive failed verification."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise VerificationError(message)


def normalized_relative_path(
    value: str, description: str, allow_trailing_slash: bool = False
) -> PurePosixPath:
    require(bool(value), f"empty {description}")
    require("\\" not in value, f"invalid {description}: {value!r}")
    require(
        re.match(r"^[A-Za-z]:", value) is None,
        f"Windows drive {description}: {value!r}",
    )
    candidate = value[:-1] if allow_trailing_slash and value.endswith("/") else value
    path = PurePosixPath(candidate)
    require(not path.is_absolute(), f"absolute {description}: {value!r}")
    require(".." not in path.parts, f"unsafe {description}: {value!r}")
    canonical = path.as_posix()
    require(
        value == canonical
        or (allow_trailing_slash and value == f"{canonical}/"),
        f"non-canonical {description}: {value!r}",
    )
    return path


def portable_path_key(path: PurePosixPath) -> str:
    return unicodedata.normalize("NFC", path.as_posix()).casefold()


def index_tar_members(
    members: list[tarfile.TarInfo], description: str
) -> dict[str, tarfile.TarInfo]:
    result = {}
    portable_names = set()
    for member in members:
        require(
            member.isfile() or member.isdir(),
            f"{description} has a link or special member: {member.name}",
        )
        normalized = normalized_relative_path(
            member.name,
            f"{description} member",
            allow_trailing_slash=member.isdir(),
        )
        name = normalized.as_posix()
        require(
            name not in result,
            f"{description} has duplicate normalized member: {name}",
        )
        portable_name = portable_path_key(normalized)
        require(
            portable_name not in portable_names,
            f"{description} has a portable path collision: {name}",
        )
        result[name] = member
        portable_names.add(portable_name)
    return result


def verify_record(
    archive: zipfile.ZipFile,
    names: list[str],
    record_member: str,
    description: str,
) -> None:
    try:
        content = archive.read(record_member).decode("utf-8")
        rows = list(csv.reader(io.StringIO(content, newline=""), strict=True))
    except (UnicodeDecodeError, csv.Error) as error:
        raise VerificationError(f"invalid RECORD in {description}: {error}") from error

    entries = {}
    for row in rows:
        require(len(row) == 3, f"invalid RECORD row in {description}: {row}")
        name, digest, size = row
        normalized_relative_path(name, "RECORD path")
        require(name not in entries, f"duplicate RECORD path in {description}: {name}")
        entries[name] = (digest, size)

    require(
        set(entries) == set(names),
        f"RECORD members differ in {description}",
    )
    record_dir = PurePosixPath(record_member).parent
    unsigned = {
        record_member,
        str(record_dir / "RECORD.jws"),
        str(record_dir / "RECORD.p7s"),
    }
    for name in names:
        digest, size = entries[name]
        if name in unsigned:
            require(not digest and not size, f"signed RECORD row in {description}: {name}")
            continue
        data = archive.read(name)
        encoded = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=")
        require(
            digest == f"sha256={encoded.decode()}",
            f"RECORD hash differs in {description}: {name}",
        )
        require(
            size.isdecimal() and int(size) == len(data),
            f"RECORD size differs in {description}: {name}",
        )


def verify_legal_bytes(content: bytes, description: str) -> None:
    require(b"\r" not in content, f"{description} contains CRLF line endings")
    lowered = content.lower()
    for placeholder in PLACEHOLDERS:
        require(
            placeholder not in lowered,
            f"{description} contains license placeholder {placeholder!r}",
        )


def verify_avro_notice(content: bytes, description: str) -> None:
    for marker in AVRO_NOTICE_MARKERS:
        require(marker in content, f"{description} is missing {marker!r}")


def verify_license_report(content: bytes, target: str, description: str) -> None:
    markers = (
        b"<strong>Artifact:</strong> <code>python</code>",
        f"<strong>Rust target:</strong> <code>{target}</code>".encode(),
        b"<strong>Root manifest:</strong> <code>bindings/python/Cargo.toml</code>",
        XZ_LICENSE_ANCHOR,
    )
    for marker in markers:
        require(marker in content, f"{description} is missing {marker!r}")
    if target.endswith("linux-gnu"):
        for marker in (b'id="bundled-oss-cpp-sdk"', b'id="bundled-curl-8.13.0"'):
            require(marker in content, f"{description} is missing {marker!r}")


def parse_metadata(content: bytes, description: str) -> Message:
    try:
        metadata = BytesParser(policy=policy.default).parsebytes(content)
    except Exception as error:
        raise VerificationError(f"invalid {description}: {error}") from error
    require(
        metadata.get("Metadata-Version") == "2.4",
        f"{description} must use Metadata-Version 2.4",
    )
    require(
        metadata.get("Name") == "pypaimon-rust",
        f"{description} has an invalid package name",
    )
    require(bool(metadata.get("Version")), f"{description} has no version")
    require(
        metadata.get("License-Expression") == "Apache-2.0",
        f"{description} has an invalid License-Expression",
    )
    license_files = metadata.get_all("License-File", [])
    require(
        sorted(license_files) == sorted(LEGAL_FILES),
        f"{description} has invalid License-File entries: {license_files}",
    )
    return metadata


def workspace_version() -> str:
    with (ROOT / "Cargo.toml").open("rb") as source:
        return str(tomllib.load(source)["workspace"]["package"]["version"])


def release_pyproject(source: str, version: str, base_version: str) -> str:
    if version == base_version:
        return source
    dynamic_version = 'dynamic = ["version"]\n'
    fixed_version = f'version = "{version}"\n'
    dynamic_count = source.count(dynamic_version)
    fixed_count = source.count(fixed_version)
    if dynamic_count == 1 and fixed_count == 0:
        return source.replace(dynamic_version, fixed_version, 1)
    require(
        dynamic_count == 0 and fixed_count == 1,
        "unexpected Python release version setting",
    )
    return source


def expected_sdist_pyproject(version: str) -> bytes:
    source = (PYTHON_DIR / "pyproject.toml").read_text(encoding="utf-8")
    source = release_pyproject(source, version, workspace_version())
    python_source = 'python-source = "python"\n'
    dependency_groups = "\n[dependency-groups]\n"
    require(source.count(python_source) == 1, "unexpected python-source setting")
    require(
        source.count(dependency_groups) == 1,
        "unexpected dependency-groups section",
    )
    source = source.replace(python_source, "", 1)
    rewrite = (
        'manifest-path = "bindings/python/Cargo.toml"\n'
        + python_source
        + dependency_groups
    )
    return source.replace(dependency_groups, rewrite, 1).encode()


def target_from_wheel_name(path: Path) -> str:
    parts = path.name.removesuffix(".whl").rsplit("-", 3)
    require(len(parts) == 4, f"invalid wheel filename: {path.name}")
    platform_tags = parts[-1].split(".")
    targets = set()
    for platform_tag in platform_tags:
        if platform_tag == "win_amd64":
            targets.add("x86_64-pc-windows-msvc")
        elif platform_tag.startswith("macosx_") and platform_tag.endswith("_x86_64"):
            targets.add("x86_64-apple-darwin")
        elif platform_tag.startswith("macosx_") and platform_tag.endswith("_arm64"):
            targets.add("aarch64-apple-darwin")
        elif re.fullmatch(
            r"(?:manylinux(?:\d{4}|_\d+_\d+)|linux)_x86_64", platform_tag
        ):
            targets.add("x86_64-unknown-linux-gnu")
        elif re.fullmatch(
            r"(?:manylinux(?:\d{4}|_\d+_\d+)|linux)_aarch64", platform_tag
        ):
            targets.add("aarch64-unknown-linux-gnu")
        else:
            raise VerificationError(
                f"unsupported wheel platform tag {platform_tag!r}: {path.name}"
            )
    require(len(targets) == 1, f"wheel has mixed platform targets: {path.name}")
    return targets.pop()


def verify_native_header(content: bytes, target: str, description: str) -> None:
    if target.endswith("linux-gnu"):
        require(content[:4] == b"\x7fELF", f"{description} is not ELF")
        require(len(content) >= 20 and content[4] == 2, f"{description} is not ELF64")
        byte_order = "little" if content[5] == 1 else "big"
        machine = int.from_bytes(content[18:20], byte_order)
        expected = 62 if target.startswith("x86_64") else 183
        require(machine == expected, f"{description} has ELF machine {machine}")
        return

    if target.endswith("windows-msvc"):
        require(content[:2] == b"MZ" and len(content) >= 64, f"{description} is not PE")
        pe_offset = int.from_bytes(content[60:64], "little")
        require(
            len(content) >= pe_offset + 6, f"{description} has a truncated PE header"
        )
        require(
            content[pe_offset : pe_offset + 4] == b"PE\0\0", f"{description} is not PE"
        )
        machine = int.from_bytes(content[pe_offset + 4 : pe_offset + 6], "little")
        require(machine == 0x8664, f"{description} has PE machine {machine:#x}")
        return

    magic = content[:4]
    if magic == b"\xcf\xfa\xed\xfe":
        byte_order = "little"
    elif magic == b"\xfe\xed\xfa\xcf":
        byte_order = "big"
    else:
        raise VerificationError(f"{description} is not 64-bit Mach-O")
    require(len(content) >= 8, f"{description} has a truncated Mach-O header")
    cpu_type = int.from_bytes(content[4:8], byte_order)
    expected = 0x01000007 if target.startswith("x86_64") else 0x0100000C
    require(cpu_type == expected, f"{description} has Mach-O CPU type {cpu_type:#x}")


def is_native_library(name: str) -> bool:
    filename = PurePosixPath(name).name.lower()
    return filename.endswith((".so", ".pyd", ".dylib", ".dll")) or ".so." in filename


def verify_wheel(path: Path) -> tuple[str, str]:
    target = target_from_wheel_name(path)
    expected_dir = PYTHON_DIR / "licenses" / target
    if not expected_dir.is_dir():
        expected_dir = PYTHON_DIR
    filename_prefix = "pypaimon_rust-"
    filename_head = path.name.removesuffix(".whl").rsplit("-", 3)[0]
    require(
        filename_head.startswith(filename_prefix),
        f"invalid wheel distribution name: {path.name}",
    )
    filename_version = filename_head.removeprefix(filename_prefix)

    try:
        archive = zipfile.ZipFile(path)
    except (OSError, zipfile.BadZipFile) as error:
        raise VerificationError(f"cannot open wheel {path}: {error}") from error

    with archive:
        infos = [info for info in archive.infolist() if not info.is_dir()]
        names = [info.filename for info in infos]
        portable_names = set()
        for info in infos:
            mode = info.external_attr >> 16
            file_type = stat.S_IFMT(mode)
            require(
                file_type in {0, stat.S_IFREG},
                f"wheel has a link or special member: {info.filename}",
            )
            normalized = normalized_relative_path(info.filename, "wheel member")
            key = portable_path_key(normalized)
            require(
                key not in portable_names,
                f"wheel has a portable path collision: {path.name}:{info.filename}",
            )
            portable_names.add(key)
        bad_member = archive.testzip()
        require(bad_member is None, f"wheel has a corrupt member: {bad_member}")

        metadata_members = [
            name for name in names if name.endswith(".dist-info/METADATA")
        ]
        require(
            len(metadata_members) == 1, f"wheel must contain one METADATA: {path.name}"
        )
        metadata_member = metadata_members[0]
        metadata = parse_metadata(archive.read(metadata_member), metadata_member)
        version = str(metadata["Version"])
        require(
            version == filename_version,
            f"wheel filename and metadata versions differ: {path.name}",
        )
        dist_info = PurePosixPath(metadata_member).parent

        wheel_members = [name for name in names if name == f"{dist_info}/WHEEL"]
        require(len(wheel_members) == 1, f"wheel metadata is incomplete: {path.name}")
        wheel_metadata = BytesParser(policy=policy.default).parsebytes(
            archive.read(wheel_members[0])
        )
        require(
            wheel_metadata.get("Root-Is-Purelib", "").lower() == "false",
            f"wheel is incorrectly marked pure: {path.name}",
        )
        record_member = f"{dist_info}/RECORD"
        require(record_member in names, f"wheel has no RECORD: {path.name}")
        verify_record(archive, names, record_member, path.name)

        license_report = None
        for license_file in metadata.get_all("License-File", []):
            relative = normalized_relative_path(license_file, "License-File entry")
            members = (
                str(relative),
                str(dist_info / "licenses" / relative),
            )
            matches = [
                name
                for name in names
                if PurePosixPath(name).name == relative.name
                and relative.name in LEGAL_FILES
            ]
            require(
                sorted(matches) == sorted(members),
                f"wheel has invalid {relative.name} members: {matches}",
            )
            expected_path = expected_dir / relative
            require(
                expected_path.is_file(),
                f"missing committed legal file: {expected_path}",
            )
            expected = expected_path.read_bytes()
            for member in members:
                actual = archive.read(member)
                require(
                    actual == expected,
                    f"wheel {member} differs for {target}",
                )
                verify_legal_bytes(actual, f"{path.name}:{member}")
                if relative.name == "NOTICE":
                    verify_avro_notice(actual, f"{path.name}:{member}")
                if relative.name == "THIRD-PARTY-LICENSES.html":
                    license_report = actual
                    verify_license_report(actual, target, f"{path.name}:{member}")

        native_members = [
            name
            for name in names
            if len(PurePosixPath(name).parts) == 2
            and PurePosixPath(name).parts[0] == "pypaimon_rust"
            and PurePosixPath(name).name.startswith("pypaimon_rust.")
            and PurePosixPath(name).suffix in {".so", ".pyd"}
        ]
        require(
            len(native_members) == 1,
            f"wheel must contain one native module: {path.name}",
        )
        native_member = native_members[0]
        expected_suffix = ".pyd" if target.endswith("windows-msvc") else ".so"
        require(
            PurePosixPath(native_member).suffix == expected_suffix,
            f"wheel has an invalid native module suffix: {native_member}",
        )
        with archive.open(native_member) as native_file:
            verify_native_header(
                native_file.read(65536), target, f"{path.name}:{native_member}"
            )

        extra_native_members = [
            name for name in names if name != native_member and is_native_library(name)
        ]
        if target.endswith("linux-gnu"):
            bridge = "pypaimon_rust/liboss_cpp_bridge.so"
            require(
                bridge in extra_native_members,
                f"wheel is missing OSS C++ bridge: {path.name}",
            )
            with archive.open(bridge) as bridge_file:
                verify_native_header(
                    bridge_file.read(65536), target, f"{path.name}:{bridge}"
                )
            extra_native_members.remove(bridge)
            openssl_version = (
                b"OpenSSL 1.1.1k  FIPS"
                if target.startswith("x86_64")
                else b"OpenSSL 1.1.1w"
            )
            openssl_libraries = (
                (r"pypaimon_rust\.libs/libcrypto-[^/]+\.so\.1\.1", openssl_version),
                (r"pypaimon_rust\.libs/libssl-[^/]+\.so\.1\.1", None),
            )
            for pattern, version_marker in openssl_libraries:
                matches = [
                    name for name in extra_native_members if re.fullmatch(pattern, name)
                ]
                require(
                    len(matches) == 1,
                    f"wheel must contain one {pattern}: {extra_native_members}",
                )
                with archive.open(matches[0]) as native_file:
                    content = native_file.read()
                    verify_native_header(
                        content,
                        target,
                        f"{path.name}:{matches[0]}",
                    )
                    if version_marker is not None:
                        require(
                            version_marker in content,
                            f"wheel libcrypto is missing {version_marker!r}",
                        )
            require(
                len(extra_native_members) == len(openssl_libraries),
                f"wheel has unexpected native libraries: {extra_native_members}",
            )
            require(
                license_report is not None
                and b'id="bundled-openssl-1.1.1"' in license_report,
                "Linux wheel license report is missing the OpenSSL anchor",
            )
        else:
            require(
                not extra_native_members,
                f"wheel has unexpected native libraries: {extra_native_members}",
            )

    return target, version


def verify_sdist(path: Path) -> str:
    try:
        archive = tarfile.open(path, mode="r:gz")
    except (OSError, tarfile.TarError) as error:
        raise VerificationError(f"cannot open sdist {path}: {error}") from error

    with archive:
        members_by_name = index_tar_members(
            archive.getmembers(), f"sdist {path.name}"
        )
        names = list(members_by_name)
        roots = {PurePosixPath(name).parts[0] for name in names}
        require(len(roots) == 1, f"sdist must have one root directory: {path.name}")
        root = roots.pop()

        def read_member(name: str) -> bytes:
            try:
                member = members_by_name[name]
            except KeyError as error:
                raise VerificationError(
                    f"sdist is missing {name}: {path.name}"
                ) from error
            require(member.isfile(), f"sdist member is not a file: {name}")
            extracted = archive.extractfile(member)
            require(extracted is not None, f"cannot read sdist member: {name}")
            return extracted.read()

        metadata = parse_metadata(read_member(f"{root}/PKG-INFO"), f"{root}/PKG-INFO")
        version = str(metadata["Version"])
        require(root == f"pypaimon_rust-{version}", f"invalid sdist root: {root}")
        require(
            path.name == f"pypaimon_rust-{version}.tar.gz",
            f"invalid sdist filename: {path.name}",
        )
        for license_file in metadata.get_all("License-File", []):
            relative = normalized_relative_path(license_file, "License-File entry")
            member = f"{root}/{relative}"
            actual = read_member(member)
            expected_path = PYTHON_DIR / relative
            require(
                expected_path.is_file(), f"missing source legal file: {expected_path}"
            )
            require(
                actual == expected_path.read_bytes(),
                f"sdist {relative} differs from source",
            )
            verify_legal_bytes(actual, f"{path.name}:{member}")

        require(
            read_member(f"{root}/Cargo.lock") == (ROOT / "Cargo.lock").read_bytes(),
            "sdist Cargo.lock differs from source",
        )
        require(
            read_member(f"{root}/pyproject.toml") == expected_sdist_pyproject(version),
            "sdist pyproject.toml has an unexpected rewrite",
        )
        target_reports = [
            name for name in names if "/bindings/python/licenses/" in name
        ]
        require(
            not target_reports,
            f"sdist contains target-specific license reports: {target_reports}",
        )
        native_members = [name for name in names if is_native_library(name)]
        require(
            not native_members, f"sdist contains native libraries: {native_members}"
        )
    return version


def collect_artifacts(inputs: list[str]) -> list[Path]:
    candidates = [Path(value) for value in inputs] or [PYTHON_DIR / "dist"]
    artifacts = []
    for candidate in candidates:
        if candidate.is_dir():
            artifacts.extend(sorted(candidate.iterdir()))
        else:
            artifacts.append(candidate)
    artifacts = [
        path
        for path in artifacts
        if path.name.endswith(".whl") or path.name.endswith(".tar.gz")
    ]
    unique = list(dict.fromkeys(path.resolve() for path in artifacts))
    require(bool(unique), "no wheel or sdist artifacts found")
    for path in unique:
        require(path.is_file(), f"artifact is not a file: {path}")
    return unique


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", nargs="*", help="artifact files or directories")
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="verify supplied artifacts without requiring the full release set",
    )
    args = parser.parse_args()

    try:
        artifacts = collect_artifacts(args.artifacts)
        targets = {}
        sdist_count = 0
        versions = set()
        for artifact in artifacts:
            if artifact.name.endswith(".whl"):
                target, version = verify_wheel(artifact)
                require(target not in targets, f"duplicate wheel target: {target}")
                targets[target] = artifact
                versions.add(version)
                print(f"verified {artifact.name} ({target})")
            else:
                versions.add(verify_sdist(artifact))
                sdist_count += 1
                print(f"verified {artifact.name} (sdist)")

        require(len(versions) == 1, f"artifact versions differ: {sorted(versions)}")
        require(sdist_count <= 1, f"expected at most one sdist, found {sdist_count}")
        if not args.allow_partial:
            missing = sorted(TARGETS - targets.keys())
            require(not missing, f"missing wheel targets: {missing}")
            require(sdist_count == 1, f"expected one sdist, found {sdist_count}")
        print(f"verified {len(artifacts)} Python release artifacts")
        return 0
    except (
        KeyError,
        OSError,
        VerificationError,
        tarfile.TarError,
        tomllib.TOMLDecodeError,
        zipfile.BadZipFile,
    ) as error:
        print(f"Python release verification failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
