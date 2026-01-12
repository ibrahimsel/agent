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

"""
Backwards compatibility module for device configuration.

This module re-exports configuration classes from the unified config module.
New code should import directly from agent.config instead.
"""

from .config import (
    MQTTConfig,
    SymphonyConfig,
    StorageConfig,
    DownloadConfig,
    ExecutorConfig,
    AgentConfig as DeviceConfig,
    ConfigurationManager,
)

__all__ = [
    "MQTTConfig",
    "SymphonyConfig",
    "StorageConfig",
    "DownloadConfig",
    "ExecutorConfig",
    "DeviceConfig",
    "ConfigurationManager",
]
