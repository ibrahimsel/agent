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

"""Device agent configuration models and loader."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class DownloadConfig:
    retries: int = 3
    timeout_seconds: int = 60
    backoff_seconds: float = 2.0


@dataclass
class ExecutorConfig:
    start_grace_seconds: int = 10
    stop_timeout_seconds: int = 10


@dataclass
class StorageConfig:
    root_dir: str = "/var/lib/muto"
    keep_releases: int = 2


@dataclass
class MQTTConfig:
    host: str = "localhost"
    port: int = 1883
    keep_alive: int = 60
    user: str = ""
    password: str = ""
    namespace: str = ""
    prefix: str = "muto"
    name: str = ""


@dataclass
class SymphonyConfig:
    mqtt: MQTTConfig = field(default_factory=MQTTConfig)
    enabled: bool = True
    topic_prefix: str = "symphony"
    request_topic: str = "coa-request"
    response_topic: str = "coa-response"
    api_url: str = "http://localhost:8082/v1alpha2/"
    provider_name: str = "providers.target.mqtt"
    broker_address: str = "tcp://localhost:1883"
    client_id: str = "symphony"
    timeout_seconds: int = 30
    auto_register: bool = False
    target_name: str = "muto-device-001"


@dataclass
class DeviceConfig:
    device_id: str = "muto-device-001"
    storage: StorageConfig = field(default_factory=StorageConfig)
    downloads: DownloadConfig = field(default_factory=DownloadConfig)
    executor: ExecutorConfig = field(default_factory=ExecutorConfig)
    symphony: SymphonyConfig = field(default_factory=SymphonyConfig)
    log_dir: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DeviceConfig":
        storage = StorageConfig(**data.get("storage", {}))
        downloads = DownloadConfig(**data.get("downloads", {}))
        executor = ExecutorConfig(**data.get("executor", {}))

        symphony_data = data.get("symphony", {})
        mqtt = MQTTConfig(**symphony_data.get("mqtt", {}))
        symphony = SymphonyConfig(mqtt=mqtt, **{k: v for k, v in symphony_data.items() if k != "mqtt"})

        device_id = data.get("device_id", symphony.target_name)
        if not symphony.mqtt.name:
            symphony.mqtt.name = device_id

        return cls(
            device_id=device_id,
            storage=storage,
            downloads=downloads,
            executor=executor,
            symphony=symphony,
            log_dir=data.get("log_dir"),
        )

    @classmethod
    def load(cls, path: Optional[str] = None) -> "DeviceConfig":
        config_path = path or os.environ.get("MUTO_CONFIG")
        if config_path and os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            return cls.from_dict(data)

        config = cls()
        env_root = os.environ.get("MUTO_ROOT")
        if env_root:
            config.storage.root_dir = env_root
        env_target = os.environ.get("MUTO_TARGET")
        if env_target:
            config.device_id = env_target
            config.symphony.target_name = env_target
            config.symphony.mqtt.name = env_target
        return config
