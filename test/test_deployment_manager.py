#
# Copyright (c) 2025 Composiv.ai
#
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# http://www.eclipse.org/legal/epl-2.0.
#
# SPDX-License-Identifier: EPL-2.0
#

import hashlib
import os
import sys
import tarfile
import tempfile
import time
import unittest
from pathlib import Path

from agent.deployment_manager import DeploymentManager
from agent.device_config import DeviceConfig, DownloadConfig, ExecutorConfig, StorageConfig
from agent.release_spec import ArtifactSpec, ReleaseSpec, RuntimeSpec


def _create_archive(tmp_dir: str, name: str) -> str:
    artifact_path = os.path.join(tmp_dir, name)
    payload_path = os.path.join(tmp_dir, "payload.txt")
    with open(payload_path, "w", encoding="utf-8") as handle:
        handle.write("ok")
    with tarfile.open(artifact_path, "w:gz") as tar_handle:
        tar_handle.add(payload_path, arcname="payload.txt")
    return artifact_path


def _sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


class TestDeploymentManager(unittest.TestCase):
    def test_atomic_install_and_activate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            archive = _create_archive(tmp_dir, "release.tar.gz")
            checksum = _sha256(archive)

            config = DeviceConfig(
                device_id="device-1",
                storage=StorageConfig(root_dir=tmp_dir),
                downloads=DownloadConfig(retries=1, timeout_seconds=5, backoff_seconds=0.1),
                executor=ExecutorConfig(start_grace_seconds=1, stop_timeout_seconds=1),
            )
            manager = DeploymentManager(config, logger=_NullLogger())
            release = ReleaseSpec(
                name="stack-a",
                version="1.0.0",
                artifact=ArtifactSpec(uri=Path(archive).as_uri(), checksum=checksum),
                runtime=RuntimeSpec(
                    start_command=f"{sys.executable} -c \"import time; time.sleep(2)\"",
                    stop_command=None,
                    environment={},
                    working_directory=".",
                ),
            )

            outcome = manager.apply_release(release)
            self.assertEqual(outcome.status, "running")

            current_link = os.path.join(tmp_dir, "stacks", "stack-a", "current")
            self.assertTrue(os.path.islink(current_link))
            self.assertEqual(os.readlink(current_link), "releases/1.0.0")

            manager.remove_release(release)
            time.sleep(0.2)

    def test_rollback_on_start_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            archive = _create_archive(tmp_dir, "release.tar.gz")
            checksum = _sha256(archive)

            config = DeviceConfig(
                device_id="device-1",
                storage=StorageConfig(root_dir=tmp_dir),
                downloads=DownloadConfig(retries=1, timeout_seconds=5, backoff_seconds=0.1),
                executor=ExecutorConfig(start_grace_seconds=1, stop_timeout_seconds=1),
            )
            manager = DeploymentManager(config, logger=_NullLogger())
            release_v1 = ReleaseSpec(
                name="stack-a",
                version="1.0.0",
                artifact=ArtifactSpec(uri=Path(archive).as_uri(), checksum=checksum),
                runtime=RuntimeSpec(
                    start_command=f"{sys.executable} -c \"import time; time.sleep(2)\"",
                    stop_command=None,
                    environment={},
                    working_directory=".",
                ),
            )
            release_v2 = ReleaseSpec(
                name="stack-a",
                version="1.0.1",
                artifact=ArtifactSpec(uri=Path(archive).as_uri(), checksum=checksum),
                runtime=RuntimeSpec(
                    start_command="false",
                    stop_command=None,
                    environment={},
                    working_directory=".",
                ),
            )

            outcome_v1 = manager.apply_release(release_v1)
            self.assertEqual(outcome_v1.status, "running")

            outcome_v2 = manager.apply_release(release_v2)
            self.assertEqual(outcome_v2.status, "rolled_back")

            current_link = os.path.join(tmp_dir, "stacks", "stack-a", "current")
            self.assertTrue(os.path.islink(current_link))
            self.assertEqual(os.readlink(current_link), "releases/1.0.0")

            manager.remove_release(release_v1)
            time.sleep(0.2)


class _NullLogger:
    def info(self, *_args, **_kwargs) -> None:
        return None

    def warning(self, *_args, **_kwargs) -> None:
        return None

    def error(self, *_args, **_kwargs) -> None:
        return None

    def debug(self, *_args, **_kwargs) -> None:
        return None
