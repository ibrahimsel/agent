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

"""State persistence for device deployments."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class StateStore:
    def __init__(self, state_path: str) -> None:
        self._state_path = state_path

    def load(self) -> Dict[str, Any]:
        if not os.path.exists(self._state_path):
            return self._default_state()
        try:
            with open(self._state_path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (OSError, json.JSONDecodeError):
            return self._default_state()
        return self._merge_defaults(data)

    def save(self, state: Dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(self._state_path), exist_ok=True)
        tmp_path = f"{self._state_path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(state, handle, indent=2, sort_keys=True)
        os.replace(tmp_path, self._state_path)

    def record_release(self, release: Dict[str, Any]) -> Dict[str, Any]:
        state = self.load()
        releases = state.setdefault("releases", {})
        releases[release["version"]] = release
        state["releases"] = releases
        self.save(state)
        return state

    def update_deployment_state(
        self,
        state_value: str,
        target_version: Optional[str] = None,
        last_failure: Optional[str] = None,
    ) -> Dict[str, Any]:
        state = self.load()
        deployment = state.setdefault("deployment", {})
        deployment["state"] = state_value
        if target_version is not None:
            deployment["target_version"] = target_version
        if last_failure is not None:
            deployment["last_failure"] = last_failure
            deployment["last_failure_at"] = _utc_now()
        state["deployment"] = deployment
        self.save(state)
        return state

    def set_current(self, current: Optional[str], previous: Optional[str]) -> Dict[str, Any]:
        state = self.load()
        state["current"] = current
        state["previous"] = previous
        state.setdefault("deployment", {}).setdefault("timestamps", {})["activated"] = _utc_now()
        self.save(state)
        return state

    def record_install_timestamp(self, version: str) -> Dict[str, Any]:
        state = self.load()
        timestamps = state.setdefault("deployment", {}).setdefault("timestamps", {})
        timestamps["installed"] = _utc_now()
        state["deployment"]["target_version"] = version
        self.save(state)
        return state

    def record_rollback_timestamp(self, version: Optional[str]) -> Dict[str, Any]:
        state = self.load()
        timestamps = state.setdefault("deployment", {}).setdefault("timestamps", {})
        timestamps["rolled_back"] = _utc_now()
        if version:
            state.setdefault("deployment", {})["target_version"] = version
        self.save(state)
        return state

    def update_process(self, pid: Optional[int], started_at: Optional[str]) -> Dict[str, Any]:
        state = self.load()
        process = state.setdefault("process", {})
        process["pid"] = pid
        process["started_at"] = started_at
        state["process"] = process
        self.save(state)
        return state

    @staticmethod
    def _default_state() -> Dict[str, Any]:
        return {
            "current": None,
            "previous": None,
            "deployment": {
                "state": "idle",
                "target_version": None,
                "last_failure": None,
                "last_failure_at": None,
                "timestamps": {},
            },
            "process": {"pid": None, "started_at": None},
            "releases": {},
        }

    def _merge_defaults(self, data: Dict[str, Any]) -> Dict[str, Any]:
        default = self._default_state()
        for key, value in default.items():
            if key not in data:
                data[key] = value
        deployment = data.setdefault("deployment", {})
        for key, value in default["deployment"].items():
            deployment.setdefault(key, value)
        data["deployment"] = deployment
        data.setdefault("process", default["process"])
        data.setdefault("releases", {})
        return data
