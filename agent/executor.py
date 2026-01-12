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

"""Runtime command executor for device agent."""

from __future__ import annotations

import os
import signal
import subprocess
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class ProcessHandle:
    popen: subprocess.Popen[str]
    log_path: str

    @property
    def pid(self) -> int:
        return self.popen.pid


class CommandExecutor:
    def start(
        self,
        command: str,
        env: Dict[str, str],
        cwd: str,
        log_path: str,
    ) -> ProcessHandle:
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        log_handle = open(log_path, "a", encoding="utf-8")
        process = subprocess.Popen(
            command,
            shell=True,
            cwd=cwd,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=log_handle,
            text=True,
        )
        log_handle.close()
        return ProcessHandle(popen=process, log_path=log_path)

    def stop(
        self,
        handle: ProcessHandle,
        stop_command: Optional[str],
        env: Dict[str, str],
        cwd: str,
        timeout_seconds: int,
    ) -> None:
        if stop_command:
            subprocess.run(
                stop_command,
                shell=True,
                cwd=cwd,
                env=env,
                timeout=timeout_seconds,
                check=False,
            )
        if handle.popen.poll() is None:
            handle.popen.terminate()
            try:
                handle.popen.wait(timeout=timeout_seconds)
            except subprocess.TimeoutExpired:
                handle.popen.kill()
                handle.popen.wait(timeout=timeout_seconds)

    @staticmethod
    def is_pid_running(pid: Optional[int]) -> bool:
        if pid is None:
            return False
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    @staticmethod
    def terminate_pid(pid: int) -> None:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            return
