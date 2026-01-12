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

"""Release metadata parsing and validation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class ArtifactSpec:
    uri: str
    checksum: str


@dataclass(frozen=True)
class RuntimeSpec:
    start_command: str
    stop_command: Optional[str]
    environment: Dict[str, str]
    working_directory: Optional[str]


@dataclass(frozen=True)
class ReleaseSpec:
    name: str
    version: str
    artifact: ArtifactSpec
    runtime: RuntimeSpec


def parse_release_payload(payload: Dict[str, Any]) -> ReleaseSpec:
    stack_props = _extract_stack_properties(payload)

    name = _first_str(
        payload.get("name"),
        stack_props.get("name"),
        _nested(payload, "metadata", "name"),
        payload.get("thingId"),
    )
    version = _first_str(
        payload.get("version"),
        stack_props.get("version"),
        _nested(payload, "metadata", "version"),
        _nested(payload, "attributes", "version"),
    )

    artifact_section = _first_dict(
        payload.get("artifact"),
        stack_props.get("artifact"),
    )
    artifact_uri = _first_str(
        payload.get("artifact_uri"),
        artifact_section.get("uri") if artifact_section else None,
    )
    checksum = _first_str(
        payload.get("checksum"),
        artifact_section.get("checksum") if artifact_section else None,
    )

    runtime_section = _first_dict(
        payload.get("runtime"),
        stack_props.get("runtime"),
    )
    start_command = _first_str(
        payload.get("start_command"),
        runtime_section.get("start_command") if runtime_section else None,
    )
    stop_command = _first_str(
        payload.get("stop_command"),
        runtime_section.get("stop_command") if runtime_section else None,
    )
    working_directory = _first_str(
        payload.get("working_directory"),
        runtime_section.get("working_directory") if runtime_section else None,
    )
    environment = _first_dict(
        payload.get("environment"),
        runtime_section.get("environment") if runtime_section else None,
    ) or {}

    if not name:
        raise ValueError("Release metadata missing name")
    if not version:
        raise ValueError("Release metadata missing version")
    if not artifact_uri:
        raise ValueError("Release metadata missing artifact uri")
    if not checksum:
        raise ValueError("Release metadata missing checksum")
    if not start_command:
        raise ValueError("Release metadata missing start_command")

    env_map = {str(k): str(v) for k, v in environment.items()}

    return ReleaseSpec(
        name=name,
        version=version,
        artifact=ArtifactSpec(uri=artifact_uri, checksum=checksum),
        runtime=RuntimeSpec(
            start_command=start_command,
            stop_command=stop_command or None,
            environment=env_map,
            working_directory=working_directory or None,
        ),
    )


def _extract_stack_properties(payload: Dict[str, Any]) -> Dict[str, Any]:
    features = payload.get("features", {})
    stack_feature = features.get("stack", {})
    props = stack_feature.get("properties")
    if isinstance(props, dict):
        return props
    stack_section = payload.get("stack")
    if isinstance(stack_section, dict):
        return stack_section
    return {}


def _nested(payload: Dict[str, Any], *keys: str) -> Optional[str]:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current if isinstance(current, str) else None


def _first_str(*values: Optional[str]) -> Optional[str]:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value
    return None


def _first_dict(*values: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for value in values:
        if isinstance(value, dict):
            return value
    return None
