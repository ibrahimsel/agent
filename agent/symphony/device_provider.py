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

"""Symphony provider for the device agent deployment lifecycle."""

from __future__ import annotations

import base64
import json
from typing import Any

from ..deployment_manager import DeploymentManager
from ..device_config import DeviceConfig
from ..heartbeat import HeartbeatConfig, HeartbeatManager
from ..release_spec import parse_release_payload
from .sdk.symphony_api import SymphonyAPIClient, SymphonyAPIError
from .sdk.symphony_sdk import ComparisonPack, ComponentSpec, SymphonyProvider, to_dict
from .sdk.symphony_summary import (
    ComponentResultSpec,
    SummarySpec,
    SummaryState,
    TargetResultSpec,
)
from .sdk.symphony_types import State


class MutoDeviceProvider(SymphonyProvider):
    def __init__(self, config: DeviceConfig, manager: DeploymentManager, logger) -> None:
        self._config = config
        self._manager = manager
        self._logger = logger
        self._api_client: SymphonyAPIClient | None = None
        self._component_registry: dict[str, dict[str, Any]] = {}
        self._heartbeat: HeartbeatManager | None = None

    def init_provider(self) -> None:
        self._logger.info("Muto device provider initialized")
        if self._config.symphony.auto_register:
            self._auto_register_target()
        self._start_heartbeat()

    def cleanup(self) -> None:
        """Clean up resources (call on shutdown)."""
        if self._heartbeat:
            self._heartbeat.stop()

    def _start_heartbeat(self) -> None:
        """Start the heartbeat manager for periodic status reporting."""
        heartbeat_config = HeartbeatConfig(
            interval_seconds=30.0,
            enabled=True,
        )
        self._heartbeat = HeartbeatManager(
            device_id=self._config.device_id,
            manager=self._manager,
            api_client=self._api_client,
            logger=self._logger,
            config=heartbeat_config,
        )
        self._heartbeat.start()

    def apply(self, metadata: dict[str, Any], components: list[ComponentSpec]) -> str:
        result = SummarySpec(target_count=1)
        target_result = TargetResultSpec()
        successes = 0
        failures = 0
        target_name = metadata.get("active-target", self._config.symphony.target_name)

        for component in components:
            component_name = component.name or "unnamed-component"
            component_result = ComponentResultSpec()

            payload, error = self._extract_stack_payload(component)
            if error:
                failures += 1
                component_result.status = State.UPDATE_FAILED
                component_result.message = error
                target_result.component_results[component_name] = component_result
                continue

            try:
                release = parse_release_payload(payload)
                self._manager.ensure_stack_ready(release.name)
                outcome = self._manager.apply_release(release)
            except Exception as exc:
                failures += 1
                component_result.status = State.UPDATE_FAILED
                component_result.message = str(exc)
                target_result.component_results[component_name] = component_result
                continue

            if outcome.status in ("running", "noop"):
                successes += 1
                component_result.status = State.UPDATED
                component_result.message = outcome.message
            else:
                failures += 1
                component_result.status = State.UPDATE_FAILED
                component_result.message = outcome.message

            target_result.component_results[component_name] = component_result
            self._component_registry[component_name] = {
                "component": to_dict(component),
                "payload": payload,
                "status": outcome.status,
                "state": component_result.status.value,
            }

        target_result.status = "OK" if failures == 0 else "FAILED"
        if failures:
            target_result.message = f"{failures} component(s) failed during apply"
            result.summary_message = target_result.message

        result.success_count = successes
        result.current_deployed = successes
        result.planned_deployment = len(components)
        target_result.state = SummaryState.DONE
        result.update_target_result(target_name, target_result)

        return json.dumps(result.to_dict(), indent=2)

    def remove(self, metadata: dict[str, Any], components: list[ComponentSpec]) -> str:
        result = SummarySpec(target_count=1, is_removal=True)
        target_result = TargetResultSpec()
        successes = 0
        failures = 0
        target_name = metadata.get("active-target", self._config.symphony.target_name)

        for component in components:
            component_name = component.name or "unnamed-component"
            component_result = ComponentResultSpec()

            payload, error = self._extract_stack_payload(component, allow_registry_lookup=True)
            if error:
                failures += 1
                component_result.status = State.DELETE_FAILED
                component_result.message = error
                target_result.component_results[component_name] = component_result
                continue

            try:
                release = parse_release_payload(payload)
                outcome = self._manager.remove_release(release)
            except Exception as exc:
                failures += 1
                component_result.status = State.DELETE_FAILED
                component_result.message = str(exc)
                target_result.component_results[component_name] = component_result
                continue

            if outcome.status in ("removed", "rolled_back", "stopped", "noop"):
                successes += 1
                component_result.status = State.DELETED
                component_result.message = outcome.message
            else:
                failures += 1
                component_result.status = State.DELETE_FAILED
                component_result.message = outcome.message

            target_result.component_results[component_name] = component_result
            self._component_registry.pop(component_name, None)

        target_result.status = "OK" if failures == 0 else "FAILED"
        if failures:
            target_result.message = f"{failures} component(s) failed during removal"
            result.summary_message = target_result.message

        result.success_count = successes
        if successes:
            result.removed = True
        target_result.state = SummaryState.DONE
        result.update_target_result(target_name, target_result)

        return json.dumps(result.to_dict(), indent=2)

    def get(self, metadata: dict[str, Any], components: list[ComponentSpec]) -> Any:
        target_name = metadata.get("active-target", self._config.symphony.target_name)
        reported: list[dict[str, Any]] = []
        if components:
            for component in components:
                payload, error = self._extract_stack_payload(component, allow_registry_lookup=True)
                if error or not payload:
                    continue
                try:
                    release = parse_release_payload(payload)
                except Exception:
                    continue
                status = self._manager.get_status(release.name)
                reported.append(
                    {
                        "component": component.name or release.name,
                        "target": target_name,
                        "release": release.version,
                        "status": status,
                    }
                )
        else:
            for component_name, registry_entry in self._component_registry.items():
                payload = registry_entry.get("payload")
                if not payload:
                    continue
                try:
                    release = parse_release_payload(payload)
                except Exception:
                    continue
                status = self._manager.get_status(release.name)
                reported.append(
                    {
                        "component": component_name,
                        "target": target_name,
                        "release": release.version,
                        "status": status,
                    }
                )
        return json.dumps(reported, indent=2)

    def needs_update(self, metadata: dict[str, Any], pack: ComparisonPack) -> bool:
        current_by_name = {comp.name: comp for comp in pack.current if comp.name}
        for desired in pack.desired:
            if not desired.name:
                continue
            current = current_by_name.get(desired.name)
            if not current:
                return True
            desired_payload, _ = self._extract_stack_payload(desired)
            current_payload, _ = self._extract_stack_payload(current)
            if not desired_payload or not current_payload:
                return True
            try:
                desired_release = parse_release_payload(desired_payload)
                current_release = parse_release_payload(current_payload)
            except Exception:
                return True
            if desired_release.version != current_release.version:
                return True
        return False

    def needs_remove(self, metadata: dict[str, Any], pack: ComparisonPack) -> bool:
        desired_names = {comp.name for comp in pack.desired if comp.name}
        return any(
            current.name and current.name not in desired_names
            for current in pack.current
        )

    def _auto_register_target(self) -> None:
        try:
            symphony = self._config.symphony
            binding_config = {
                "name": "proxy",
                "brokerAddress": symphony.broker_address,
                "clientID": symphony.client_id,
                "requestTopic": f"{symphony.topic_prefix}/{symphony.request_topic}",
                "responseTopic": f"{symphony.topic_prefix}/{symphony.response_topic}",
                "timeoutSeconds": str(symphony.timeout_seconds),
            }
            self._api_client = SymphonyAPIClient(
                base_url=symphony.api_url,
                username=symphony.mqtt.user,
                password=symphony.mqtt.password,
                timeout=30.0,
                logger=self._logger,
            )
            payload = {
                "metadata": {"name": symphony.target_name},
                "spec": {
                    "displayName": symphony.target_name,
                    "forceRedeploy": True,
                    "components": [
                        {
                            "name": "muto",
                            "type": "muto-agent",
                            "properties": {},
                        }
                    ],
                    "topologies": [
                        {
                            "bindings": [
                                {
                                    "role": "instance",
                                    "provider": symphony.provider_name,
                                    "config": binding_config,
                                },
                                {
                                    "role": "muto-agent",
                                    "provider": symphony.provider_name,
                                    "config": binding_config,
                                },
                            ]
                        }
                    ],
                },
            }
            self._api_client.register_target(symphony.target_name, payload)
            self._logger.info("Symphony target registered: %s", symphony.target_name)
        except SymphonyAPIError as exc:
            self._logger.error("Symphony auto-register failed: %s", exc)

    def _extract_stack_payload(  # noqa: PLR0911
        self,
        component: ComponentSpec,
        allow_registry_lookup: bool = False,
    ) -> tuple[Any | None, str | None]:
        try:
            props = component.properties or {}
            data = props.get("data")

            if data is None and allow_registry_lookup:
                registry_entry = self._component_registry.get(component.name or "")
                if registry_entry:
                    return registry_entry.get("payload"), None
                return None, "Component stack payload not available"

            if isinstance(data, dict):
                return data, None

            if isinstance(data, bytes):
                decoded_bytes = data
            elif isinstance(data, str):
                decoded_bytes = self._attempt_base64_decode(data) or data.encode("utf-8")
            else:
                return None, "Unsupported payload format"

            payload_str = decoded_bytes.decode("utf-8")
            return json.loads(payload_str), None
        except json.JSONDecodeError as exc:
            return None, f"Failed to parse stack data: {exc}"
        except Exception as exc:
            return None, f"Unexpected error reading stack payload: {exc}"

    @staticmethod
    def _attempt_base64_decode(data: str) -> bytes | None:
        try:
            return base64.b64decode(data)
        except Exception:
            return None
