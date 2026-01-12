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

"""Heartbeat manager for periodic Symphony status reporting."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import logging

    from .deployment_manager import DeploymentManager
    from .symphony.sdk.symphony_api import SymphonyAPIClient


@dataclass
class HeartbeatConfig:
    """Configuration for heartbeat manager."""

    interval_seconds: float = 30.0
    enabled: bool = True


@dataclass
class DeviceStatus:
    """Current device status for heartbeat reporting."""

    device_id: str
    online: bool = True
    stacks: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for API submission."""
        return {
            "device_id": self.device_id,
            "online": self.online,
            "stacks": self.stacks or {},
            "timestamp": time.time(),
        }


class HeartbeatManager:
    """Manages periodic status reporting to Symphony control plane."""

    def __init__(
        self,
        device_id: str,
        manager: DeploymentManager,
        api_client: SymphonyAPIClient | None,
        logger: logging.Logger,
        config: HeartbeatConfig | None = None,
    ) -> None:
        """Initialize heartbeat manager.

        Args:
            device_id: Unique identifier for this device.
            manager: Deployment manager to query stack status.
            api_client: Symphony API client for reporting status.
            logger: Logger instance.
            config: Heartbeat configuration.
        """
        self._device_id = device_id
        self._manager = manager
        self._api_client = api_client
        self._logger = logger
        self._config = config or HeartbeatConfig()
        self._shutdown_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._status_callback: Callable[[DeviceStatus], None] | None = None

    def start(self) -> None:
        """Start the heartbeat background thread."""
        if not self._config.enabled:
            self._logger.info("Heartbeat disabled by configuration")
            return

        if self._thread is not None and self._thread.is_alive():
            self._logger.warning("Heartbeat already running")
            return

        self._shutdown_event.clear()
        self._thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._thread.start()
        self._logger.info(
            "Heartbeat started with interval %ss", self._config.interval_seconds
        )

    def stop(self) -> None:
        """Stop the heartbeat background thread."""
        self._shutdown_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
        self._logger.info("Heartbeat stopped")

    def set_status_callback(
        self, callback: Callable[[DeviceStatus], None]
    ) -> None:
        """Set callback for status updates (useful for MQTT publishing)."""
        self._status_callback = callback

    def _heartbeat_loop(self) -> None:
        """Background loop that sends periodic heartbeats."""
        while not self._shutdown_event.is_set():
            try:
                status = self._collect_status()
                self._report_status(status)
            except Exception as exc:
                self._logger.error("Heartbeat error: %s", exc)

            self._shutdown_event.wait(timeout=self._config.interval_seconds)

    def _collect_status(self) -> DeviceStatus:
        """Collect current device status from deployment manager."""
        stacks: dict[str, Any] = {}

        try:
            # Get status for all known stacks
            all_stacks = self._manager.list_stacks()
            for stack_name in all_stacks:
                stack_status = self._manager.get_status(stack_name)
                stacks[stack_name] = stack_status
        except Exception as exc:
            self._logger.warning("Failed to collect stack status: %s", exc)

        return DeviceStatus(
            device_id=self._device_id,
            online=True,
            stacks=stacks,
        )

    def _report_status(self, status: DeviceStatus) -> None:
        """Report status to Symphony and/or via callback."""
        status_dict = status.to_dict()

        # Report via callback (e.g., MQTT)
        if self._status_callback:
            try:
                self._status_callback(status)
            except Exception as exc:
                self._logger.warning("Status callback failed: %s", exc)

        # Report via Symphony API if available
        if self._api_client:
            try:
                self._api_client.update_target_status(
                    self._device_id, status_dict
                )
            except Exception as exc:
                self._logger.debug("Symphony status update failed: %s", exc)
