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

"""Deployment orchestration for atomic installs and rollback."""

from __future__ import annotations

import contextlib
import os
import shutil
import signal
import subprocess
import threading
import time
from dataclasses import dataclass

from .artifact import download_with_checksum, extract_archive
from .device_config import DeviceConfig
from .executor import CommandExecutor, ProcessHandle
from .release_spec import ArtifactSpec, ReleaseSpec, RuntimeSpec
from .state_store import StateStore, _utc_now


@dataclass
class DeploymentOutcome:
    status: str
    message: str
    version: str | None = None


@dataclass
class StackPaths:
    root_dir: str
    stack_dir: str
    releases_dir: str
    incoming_dir: str
    logs_dir: str
    state_file: str

    def release_dir(self, version: str) -> str:
        return os.path.join(self.releases_dir, version)

    def release_tmp_dir(self, version: str) -> str:
        return os.path.join(self.releases_dir, f"{version}.tmp")


class DeploymentManager:
    def __init__(self, config: DeviceConfig, logger) -> None:
        print("Initializing DeploymentManager with config:", config)
        self._config = config
        self._logger = logger
        self._executor = CommandExecutor()
        self._locks: dict[str, threading.Lock] = {}

    def ensure_stack_ready(self, stack_name: str) -> None:
        paths = self._stack_paths(stack_name)
        os.makedirs(paths.releases_dir, exist_ok=True)
        os.makedirs(paths.incoming_dir, exist_ok=True)
        os.makedirs(paths.logs_dir, exist_ok=True)
        self._cleanup_tmp_dirs(paths)

    def apply_release(self, release: ReleaseSpec) -> DeploymentOutcome:
        lock = self._locks.setdefault(release.name, threading.Lock())
        with lock:
            paths = self._stack_paths(release.name)
            self.ensure_stack_ready(release.name)
            store = StateStore(paths.state_file)
            state = store.load()
            store.record_release(self._release_to_dict(release))

            current = state.get("current")
            pid = state.get("process", {}).get("pid")
            if current == release.version and self._executor.is_pid_running(pid):
                return DeploymentOutcome(
                    status="noop",
                    message="Release already active",
                    version=release.version,
                )

            if not os.path.isdir(paths.release_dir(release.version)):
                store.update_deployment_state("installing", target_version=release.version)
                try:
                    self._install_release(release, paths, store)
                except Exception as exc:
                    store.update_deployment_state("failed", release.version, str(exc))
                    return DeploymentOutcome("failed", str(exc), release.version)

            previous = state.get("previous")
            if current and current != release.version:
                previous = current
            if current and current != release.version:
                existing_release = self._release_from_state(state, previous)
                if existing_release:
                    self._stop_current_process(state, existing_release, paths, store)
            store.update_deployment_state("activating", target_version=release.version)
            try:
                self._activate_release(paths, release.version)
                store.set_current(release.version, previous)
            except Exception as exc:
                store.update_deployment_state("failed", release.version, str(exc))
                return DeploymentOutcome("failed", str(exc), release.version)

            outcome = self._start_release(release, paths, store, previous)
            if outcome.status != "running":
                return outcome

            store.update_deployment_state("running", release.version, None)
            return DeploymentOutcome("running", "Release activated", release.version)

    def remove_release(self, release: ReleaseSpec) -> DeploymentOutcome:
        lock = self._locks.setdefault(release.name, threading.Lock())
        with lock:
            paths = self._stack_paths(release.name)
            store = StateStore(paths.state_file)
            state = store.load()
            current = state.get("current")
            previous = state.get("previous")

            if current != release.version:
                if os.path.isdir(paths.release_dir(release.version)):
                    shutil.rmtree(paths.release_dir(release.version), ignore_errors=True)
                    return DeploymentOutcome(
                        "removed", "Release directory removed", release.version
                    )
                return DeploymentOutcome("noop", "Release not active", release.version)

            self._stop_current_process(state, release, paths, store)

            self._remove_symlink(os.path.join(paths.stack_dir, "current"))
            store.set_current(None, previous)
            return DeploymentOutcome("stopped", "Release stopped", release.version)

    def get_status(self, stack_name: str) -> dict[str, str | None]:
        paths = self._stack_paths(stack_name)
        store = StateStore(paths.state_file)
        state = store.load()
        deployment = state.get("deployment", {})
        return {
            "current": state.get("current"),
            "previous": state.get("previous"),
            "deployment_state": deployment.get("state"),
            "last_failure": deployment.get("last_failure"),
            "last_failure_at": deployment.get("last_failure_at"),
            "installed_at": deployment.get("timestamps", {}).get("installed"),
            "activated_at": deployment.get("timestamps", {}).get("activated"),
            "rolled_back_at": deployment.get("timestamps", {}).get("rolled_back"),
        }

    def list_stacks(self) -> list[str]:
        """List all known stack names on this device."""
        stacks_root = os.path.join(self._config.storage.root_dir, "stacks")
        if not os.path.isdir(stacks_root):
            return []
        return [
            entry
            for entry in os.listdir(stacks_root)
            if os.path.isdir(os.path.join(stacks_root, entry))
        ]

    def restart_current_if_needed(self, stack_name: str) -> None:
        paths = self._stack_paths(stack_name)
        store = StateStore(paths.state_file)
        state = store.load()
        current = state.get("current")
        if not current:
            return
        pid = state.get("process", {}).get("pid")
        if self._executor.is_pid_running(pid):
            return
        release = self._release_from_state(state, current)
        if not release:
            return
        self._start_release(release, paths, store, None)

    def cleanup_incomplete_installations(self, stack_name: str) -> None:
        paths = self._stack_paths(stack_name)
        self._cleanup_tmp_dirs(paths)

    def _stack_paths(self, stack_name: str) -> StackPaths:
        root_dir = self._config.storage.root_dir
        stack_dir = os.path.join(root_dir, "stacks", stack_name)
        releases_dir = os.path.join(stack_dir, "releases")
        incoming_dir = os.path.join(root_dir, "incoming")
        logs_dir = os.path.join(stack_dir, "logs")
        state_file = os.path.join(stack_dir, "state.json")
        return StackPaths(
            root_dir=root_dir,
            stack_dir=stack_dir,
            releases_dir=releases_dir,
            incoming_dir=incoming_dir,
            logs_dir=logs_dir,
            state_file=state_file,
        )

    def _install_release(self, release: ReleaseSpec, paths: StackPaths, store: StateStore) -> None:
        version = release.version
        incoming_file = os.path.join(paths.incoming_dir, f"{release.name}-{version}.artifact")
        download_with_checksum(
            release.artifact.uri,
            incoming_file,
            release.artifact.checksum,
            retries=self._config.downloads.retries,
            timeout_seconds=self._config.downloads.timeout_seconds,
            backoff_seconds=self._config.downloads.backoff_seconds,
        )
        store.record_install_timestamp(version)
        tmp_dir = paths.release_tmp_dir(version)
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir, ignore_errors=True)
        os.makedirs(tmp_dir, exist_ok=False)
        try:
            extract_archive(incoming_file, tmp_dir)
            if not os.listdir(tmp_dir):
                raise RuntimeError("Extracted release directory is empty")
            os.rename(tmp_dir, paths.release_dir(version))
        except Exception:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            raise

    def _activate_release(self, paths: StackPaths, version: str) -> None:
        release_dir = paths.release_dir(version)
        if not os.path.isdir(release_dir):
            raise RuntimeError(f"Release directory {release_dir} not found")

        current_link = os.path.join(paths.stack_dir, "current")
        previous_link = os.path.join(paths.stack_dir, "previous")
        current_target = self._read_symlink(current_link)
        if current_target and current_target != f"releases/{version}":
            self._atomic_symlink(current_target, previous_link)

        if current_target == f"releases/{version}":
            return
        self._atomic_symlink(f"releases/{version}", current_link)

    def _start_release(
        self,
        release: ReleaseSpec,
        paths: StackPaths,
        store: StateStore,
        previous_version: str | None,
    ) -> DeploymentOutcome:
        store.update_deployment_state("starting", release.version, None)
        release_dir = paths.release_dir(release.version)
        cwd = self._resolve_cwd(release_dir, release.runtime.working_directory)
        env = dict(os.environ)
        env.update(release.runtime.environment)
        log_path = os.path.join(paths.logs_dir, f"{release.version}.log")
        try:
            handle = self._executor.start(
                release.runtime.start_command, env, cwd, log_path
            )
        except Exception as exc:
            store.update_deployment_state("failed", release.version, str(exc))
            if not previous_version:
                return DeploymentOutcome("failed", f"Start failed: {exc}", release.version)
            error_msg = f"Start failed: {exc}"
            return self._rollback_to(previous_version, release, paths, store, error_msg)

        store.update_process(handle.pid, _utc_now())
        failed, reason = self._wait_for_grace(handle, release)
        if failed:
            store.update_deployment_state("failed", release.version, reason)
            if not previous_version:
                return DeploymentOutcome("failed", reason, release.version)
            return self._rollback_to(previous_version, release, paths, store, reason)

        return DeploymentOutcome("running", "Release started", release.version)

    def _wait_for_grace(self, handle: ProcessHandle, release: ReleaseSpec) -> tuple[bool, str]:
        deadline = time.monotonic() + self._config.executor.start_grace_seconds
        while time.monotonic() < deadline:
            exit_code = handle.popen.poll()
            if exit_code is not None:
                if exit_code != 0:
                    return True, f"Process exited with {exit_code}"
                return True, "Process exited during grace period"
            time.sleep(0.5)
        return False, ""

    def _rollback_to(
        self,
        previous_version: str | None,
        failed_release: ReleaseSpec,
        paths: StackPaths,
        store: StateStore,
        reason: str,
    ) -> DeploymentOutcome:
        self._logger.error("Deployment failed for %s: %s", failed_release.name, reason)
        if not previous_version:
            store.update_deployment_state("failed", failed_release.version, reason)
            return DeploymentOutcome("failed", reason, failed_release.version)

        rollback_release = self._release_from_state(store.load(), previous_version)
        if not rollback_release:
            store.update_deployment_state("failed", failed_release.version, reason)
            return DeploymentOutcome("failed", reason, failed_release.version)

        store.update_deployment_state("rollback", previous_version, reason)
        self._activate_release(paths, previous_version)
        store.set_current(previous_version, failed_release.version)
        store.record_rollback_timestamp(previous_version)
        restart_outcome = self._start_release(rollback_release, paths, store, None)
        if restart_outcome.status == "running":
            msg = f"Rollback succeeded: {reason}"
            return DeploymentOutcome("rolled_back", msg, previous_version)
        return DeploymentOutcome("failed", f"Rollback failed: {reason}", previous_version)

    def _stop_current_process(
        self,
        state: dict[str, str | None],
        release: ReleaseSpec,
        paths: StackPaths,
        store: StateStore,
    ) -> None:
        process_data = state.get("process")
        pid = process_data.get("pid") if isinstance(process_data, dict) else None
        if not pid:
            return
        release_dir = paths.release_dir(release.version)
        cwd = self._resolve_cwd(release_dir, release.runtime.working_directory)
        env = dict(os.environ)
        env.update(release.runtime.environment)
        if release.runtime.stop_command:
            try:
                self._executor.stop(
                    ProcessHandle(popen=_DummyProcess(pid), log_path=""),
                    release.runtime.stop_command,
                    env,
                    cwd,
                    timeout_seconds=self._config.executor.stop_timeout_seconds,
                )
            except Exception:
                self._executor.terminate_pid(pid)
        if self._executor.is_pid_running(pid):
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, signal.SIGTERM)
            deadline = time.monotonic() + self._config.executor.stop_timeout_seconds
            while time.monotonic() < deadline:
                if not self._executor.is_pid_running(pid):
                    break
                time.sleep(0.1)
            if self._executor.is_pid_running(pid):
                with contextlib.suppress(ProcessLookupError):
                    os.kill(pid, signal.SIGKILL)
        store.update_process(None, None)

    def _cleanup_tmp_dirs(self, paths: StackPaths) -> None:
        if not os.path.isdir(paths.releases_dir):
            return
        for entry in os.listdir(paths.releases_dir):
            if entry.endswith(".tmp"):
                shutil.rmtree(os.path.join(paths.releases_dir, entry), ignore_errors=True)

    def _release_to_dict(self, release: ReleaseSpec) -> dict[str, object]:
        return {
            "name": release.name,
            "version": release.version,
            "artifact_uri": release.artifact.uri,
            "checksum": release.artifact.checksum,
            "runtime": {
                "start_command": release.runtime.start_command,
                "stop_command": release.runtime.stop_command,
                "environment": release.runtime.environment,
                "working_directory": release.runtime.working_directory,
            },
        }

    def _release_from_state(self, state: dict[str, object], version: str) -> ReleaseSpec | None:
        releases = state.get("releases", {}) if isinstance(state.get("releases"), dict) else {}
        data = releases.get(version)
        if not isinstance(data, dict):
            return None
        runtime = data.get("runtime", {})
        return ReleaseSpec(
            name=str(data.get("name", "")),
            version=str(data.get("version", "")),
            artifact=ArtifactSpec(
                uri=str(data.get("artifact_uri", "")),
                checksum=str(data.get("checksum", "")),
            ),
            runtime=RuntimeSpec(
                start_command=str(runtime.get("start_command", "")),
                stop_command=runtime.get("stop_command"),
                environment=runtime.get("environment") or {},
                working_directory=runtime.get("working_directory"),
            ),
        )

    def _resolve_cwd(self, release_dir: str, working_directory: str | None) -> str:
        if not working_directory:
            return release_dir
        if os.path.isabs(working_directory):
            return working_directory
        return os.path.join(release_dir, working_directory)

    def _atomic_symlink(self, target: str, link_path: str) -> None:
        temp_link = f"{link_path}.tmp"
        if os.path.islink(temp_link) or os.path.exists(temp_link):
            os.unlink(temp_link)
        os.symlink(target, temp_link)
        os.replace(temp_link, link_path)

    def _read_symlink(self, link_path: str) -> str | None:
        if os.path.islink(link_path):
            return os.readlink(link_path)
        return None

    def _remove_symlink(self, link_path: str) -> None:
        if os.path.islink(link_path):
            os.unlink(link_path)


class _DummyProcess:
    def __init__(self, pid: int) -> None:
        self.pid = pid

    def poll(self) -> int | None:
        try:
            os.kill(self.pid, 0)
            return None
        except ProcessLookupError:
            return 0
        except PermissionError:
            return None

    def terminate(self) -> None:
        os.kill(self.pid, signal.SIGTERM)

    def kill(self) -> None:
        os.kill(self.pid, signal.SIGKILL)

    def wait(self, timeout: float | None = None) -> int:
        deadline = time.monotonic() + timeout if timeout else None
        while True:
            if self.poll() is not None:
                return 0
            if deadline and time.monotonic() >= deadline:
                raise subprocess.TimeoutExpired(cmd=str(self.pid), timeout=timeout)
            time.sleep(0.1)
