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

"""Device agent entrypoint for Symphony-driven deployments."""

from __future__ import annotations

import logging
import os
import signal
import threading
from typing import Optional

from .deployment_manager import DeploymentManager
from .device_config import DeviceConfig
from .symphony.device_provider import MutoDeviceProvider
from .symphony.symphony_broker import MQTTBroker


class _LogNode:
    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def get_logger(self) -> logging.Logger:
        return self._logger


def _bootstrap_existing_stacks(manager: DeploymentManager, root_dir: str) -> None:
    stacks_root = os.path.join(root_dir, "stacks")
    if not os.path.isdir(stacks_root):
        return
    for entry in os.listdir(stacks_root):
        stack_path = os.path.join(stacks_root, entry)
        if not os.path.isdir(stack_path):
            continue
        manager.cleanup_incomplete_installations(entry)
        manager.restart_current_if_needed(entry)


def main(config_path: Optional[str] = None) -> None:
    logging.basicConfig(
        level=os.environ.get("MUTO_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger = logging.getLogger("muto.device_agent")
    config = DeviceConfig.load(config_path)
    manager = DeploymentManager(config, logger)
    provider = MutoDeviceProvider(config, manager, logger)

    if not config.symphony.enabled:
        logger.error("Symphony integration disabled; exiting")
        return

    node = _LogNode(logger)
    broker = MQTTBroker(plugin=provider, node=node, config=config)
    broker.connect()

    _bootstrap_existing_stacks(manager, config.storage.root_dir)

    shutdown_event = threading.Event()

    def _handle_signal(signum, frame) -> None:
        logger.info("Received signal %s, shutting down", signum)
        shutdown_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    shutdown_event.wait()
    provider.cleanup()
    broker.stop()


if __name__ == "__main__":
    main()
