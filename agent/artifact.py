#
# Copyright (c) 2025 Composiv.ai
#
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# http://www.eclipse.org/legal/epl-2.0.
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#   Composiv.ai - initial API and implementation
#

"""Artifact download and extraction helpers."""

from __future__ import annotations

import hashlib
import os
import tarfile
import time
import urllib.request
import zipfile
from typing import Optional


def parse_sha256(checksum: str) -> str:
    if ":" in checksum and not checksum.startswith("sha256:"):
        raise ValueError("Only sha256 checksums are supported")
    if checksum.startswith("sha256:"):
        return checksum.split("sha256:", 1)[1]
    return checksum


def download_with_checksum(
    uri: str,
    destination: str,
    checksum: str,
    retries: int,
    timeout_seconds: int,
    backoff_seconds: float,
) -> None:
    expected = parse_sha256(checksum).lower()
    last_error: Optional[Exception] = None
    for attempt in range(retries):
        tmp_path = f"{destination}.part"
        try:
            _download(uri, tmp_path, expected, timeout_seconds)
            os.replace(tmp_path, destination)
            return
        except Exception as exc:
            last_error = exc
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            if attempt < retries - 1:
                time.sleep(backoff_seconds * (2**attempt))
    raise RuntimeError(f"Failed to download artifact: {last_error}")


def _download(uri: str, tmp_path: str, expected: str, timeout_seconds: int) -> None:
    digest = hashlib.sha256()
    request = urllib.request.Request(uri)
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        with open(tmp_path, "wb") as handle:
            while True:
                chunk = response.read(8192)
                if not chunk:
                    break
                handle.write(chunk)
                digest.update(chunk)
    actual = digest.hexdigest().lower()
    if actual != expected:
        raise ValueError(
            f"Checksum mismatch for {uri}: expected {expected}, got {actual}"
        )


def extract_archive(archive_path: str, destination: str) -> None:
    if tarfile.is_tarfile(archive_path):
        with tarfile.open(archive_path, "r:*") as tar_handle:
            _safe_extract_tar(tar_handle, destination)
    elif zipfile.is_zipfile(archive_path):
        with zipfile.ZipFile(archive_path, "r") as zip_handle:
            _safe_extract_zip(zip_handle, destination)
    else:
        raise ValueError(f"Unsupported archive format: {archive_path}")


def _safe_extract_tar(tar_handle: tarfile.TarFile, destination: str) -> None:
    for member in tar_handle.getmembers():
        member_path = os.path.join(destination, member.name)
        if not _is_within_directory(destination, member_path):
            raise ValueError("Blocked archive entry outside destination")
    tar_handle.extractall(destination)


def _safe_extract_zip(zip_handle: zipfile.ZipFile, destination: str) -> None:
    for member in zip_handle.namelist():
        member_path = os.path.join(destination, member)
        if not _is_within_directory(destination, member_path):
            raise ValueError("Blocked archive entry outside destination")
    zip_handle.extractall(destination)


def _is_within_directory(directory: str, target: str) -> bool:
    directory = os.path.abspath(directory)
    target = os.path.abspath(target)
    return os.path.commonpath([directory, target]) == directory
