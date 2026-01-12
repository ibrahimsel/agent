#
#  Copyright (c) 2023 Composiv.ai
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v2.0
# and Eclipse Distribution License v1.0 which accompany this distribution.
#
# Licensed under the  Eclipse Public License v2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# The Eclipse Public License is available at
#    http://www.eclipse.org/legal/epl-v20.html
# and the Eclipse Distribution License is available at
#   http://www.eclipse.org/org/documents/edl-v10.php.
#
# Contributors:
#    Composiv.ai - initial API and implementation
#
#

"""
Unified configuration management for the Muto Agent system.

This module provides configuration for both ROS and non-ROS modes.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional
from dataclasses import dataclass, field

from .exceptions import ConfigurationError


@dataclass
class MQTTConfig:
    """Configuration for MQTT connection."""
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
    """Configuration for Symphony connection."""
    mqtt: MQTTConfig = field(default_factory=MQTTConfig)
    target_name: str = "muto-device-001"
    enabled: bool = True
    topic_prefix: str = "symphony"
    api_url: str = "http://localhost:8082/v1alpha2/"
    provider_name: str = "providers.target.mqtt"
    broker_address: str = "tcp://localhost:1883"
    client_id: str = "symphony"
    request_topic: str = "coa-request"
    response_topic: str = "coa-response"
    timeout_seconds: int = 30
    auto_register: bool = False


@dataclass
class TopicConfig:
    """Configuration for ROS topics."""
    stack_topic: str = "stack"
    twin_topic: str = "twin"
    agent_to_gateway_topic: str = "agent_to_gateway"
    gateway_to_agent_topic: str = "gateway_to_agent"
    agent_to_commands_topic: str = "agent_to_command"
    commands_to_agent_topic: str = "command_to_agent"
    thing_messages_topic: str = "thing_messages"


@dataclass
class StorageConfig:
    """Configuration for local storage."""
    root_dir: str = "/var/lib/muto"
    keep_releases: int = 2


@dataclass
class DownloadConfig:
    """Configuration for artifact downloads."""
    retries: int = 3
    timeout_seconds: int = 60
    backoff_seconds: float = 2.0


@dataclass
class ExecutorConfig:
    """Configuration for process execution."""
    start_grace_seconds: int = 10
    stop_timeout_seconds: int = 10


@dataclass
class AgentConfig:
    """
    Unified configuration for the Muto Agent.

    Supports both ROS mode (with topics) and standalone mode (Symphony-only).
    """
    device_id: str = "muto-device-001"
    mqtt: MQTTConfig = field(default_factory=MQTTConfig)
    topics: TopicConfig = field(default_factory=TopicConfig)
    symphony: SymphonyConfig = field(default_factory=SymphonyConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    downloads: DownloadConfig = field(default_factory=DownloadConfig)
    executor: ExecutorConfig = field(default_factory=ExecutorConfig)
    log_dir: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AgentConfig":
        """
        Create an AgentConfig from a dictionary (e.g., parsed JSON).

        Args:
            data: Configuration dictionary.

        Returns:
            AgentConfig instance.
        """
        mqtt = MQTTConfig(**data.get("mqtt", {}))
        topics = TopicConfig(**data.get("topics", {}))
        storage = StorageConfig(**data.get("storage", {}))
        downloads = DownloadConfig(**data.get("downloads", {}))
        executor = ExecutorConfig(**data.get("executor", {}))

        symphony_data = data.get("symphony", {})
        symphony_mqtt_data = symphony_data.get("mqtt", {})
        symphony_mqtt = MQTTConfig(**symphony_mqtt_data)
        symphony = SymphonyConfig(
            mqtt=symphony_mqtt,
            **{k: v for k, v in symphony_data.items() if k != "mqtt"}
        )

        device_id = data.get("device_id", symphony.target_name)
        if not symphony.mqtt.name:
            symphony.mqtt.name = device_id

        return cls(
            device_id=device_id,
            mqtt=mqtt,
            topics=topics,
            symphony=symphony,
            storage=storage,
            downloads=downloads,
            executor=executor,
            log_dir=data.get("log_dir"),
        )

    @classmethod
    def load(cls, path: Optional[str] = None) -> "AgentConfig":
        """
        Load configuration from JSON file or environment variables.

        This method supports standalone (non-ROS) mode.

        Args:
            path: Optional path to JSON config file. If not provided,
                  checks MUTO_CONFIG environment variable.

        Returns:
            AgentConfig instance.
        """
        config_path = path or os.environ.get("MUTO_CONFIG")
        if config_path and os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            return cls.from_dict(data)

        # Create default config and apply environment overrides
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


class ConfigurationManager:
    """
    Manages configuration loading and validation for the Muto Agent system.

    This class centralizes all configuration management, providing a clean
    interface for accessing configuration parameters with proper validation
    and error handling.

    Supports both ROS mode (loading from ROS parameters) and standalone mode
    (loading from JSON file or environment variables).
    """

    def __init__(self, node=None, config_path: Optional[str] = None):
        """
        Initialize the configuration manager.

        Args:
            node: Optional ROS node to read parameters from. If None,
                  uses JSON/environment configuration.
            config_path: Optional path to JSON config file for non-ROS mode.
        """
        self._node = node
        self._config_path = config_path
        self._config: Optional[AgentConfig] = None

    def load_config(self) -> AgentConfig:
        """
        Load configuration from ROS parameters or JSON file.

        Returns:
            AgentConfig: The loaded configuration.

        Raises:
            ConfigurationError: If configuration loading fails.
        """
        if self._node is not None:
            return self._load_from_ros()
        else:
            return self._load_from_file()

    def _load_from_file(self) -> AgentConfig:
        """Load configuration from JSON file or environment."""
        try:
            self._config = AgentConfig.load(self._config_path)
            return self._config
        except Exception as e:
            raise ConfigurationError(f"Failed to load configuration: {e}") from e

    def _load_from_ros(self) -> AgentConfig:
        """Load configuration from ROS parameters."""
        try:
            # Import rclpy only when needed (ROS mode)
            import rclpy

            # Declare all parameters with defaults
            self._declare_parameters()

            # Load MQTT configuration
            mqtt_config = MQTTConfig(
                host=self._get_parameter("host", "sandbox.composiv.ai"),
                port=self._get_parameter("port", 1883),
                keep_alive=self._get_parameter("keep_alive", 60),
                user=self._get_parameter("user", ""),
                password=self._get_parameter("password", ""),
                namespace=self._get_parameter("namespace", ""),
                prefix=self._get_parameter("prefix", "muto"),
                name=self._get_parameter("name", "")
            )

            sym_mqtt_config = MQTTConfig(
                host=self._get_parameter("symphony_host", "localhost"),
                port=self._get_parameter("symphony_port", 1883),
                keep_alive=self._get_parameter("symphony_keep_alive", 60),
                user=self._get_parameter("symphony_user", ""),
                password=self._get_parameter("symphony_password", ""),
                namespace=self._get_parameter("symphony_namespace", ""),
                prefix=self._get_parameter("symphony_prefix", "muto"),
                name=self._get_parameter("symphony_name", "")
            )

            symphony_config = SymphonyConfig(
                mqtt=sym_mqtt_config,
                target_name=self._get_parameter("symphony_target_name", "muto-device-001"),
                enabled=self._get_parameter("symphony_enabled", False),
                topic_prefix=self._get_parameter("symphony_topic_prefix", "symphony"),
                api_url=self._get_parameter("symphony_api_url", "http://localhost:8082/v1alpha2/"),
                provider_name=self._get_parameter("symphony_provider_name", "providers.target.mqtt"),
                broker_address=self._get_parameter("symphony_broker_address", "tcp://localhost:1883"),
                client_id=self._get_parameter("symphony_client_id", "symphony"),
                request_topic=self._get_parameter("symphony_request_topic", "coa-request"),
                response_topic=self._get_parameter("symphony_response_topic", "coa-response"),
                timeout_seconds=self._get_parameter("symphony_timeout_seconds", 30),
                auto_register=self._get_parameter("symphony_auto_register", False),
            )

            # Load topic configuration
            topic_config = TopicConfig(
                stack_topic=self._get_parameter("stack_topic", "stack"),
                twin_topic=self._get_parameter("twin_topic", "twin"),
                agent_to_gateway_topic=self._get_parameter("agent_to_gateway_topic", "agent_to_gateway"),
                gateway_to_agent_topic=self._get_parameter("gateway_to_agent_topic", "gateway_to_agent"),
                agent_to_commands_topic=self._get_parameter("agent_to_commands_topic", "agent_to_command"),
                commands_to_agent_topic=self._get_parameter("commands_to_agent_topic", "command_to_agent"),
                thing_messages_topic=self._get_parameter("thing_messages_topic", "thing_messages")
            )

            # Load storage configuration
            storage_config = StorageConfig(
                root_dir=self._get_parameter("storage_root_dir", "/var/lib/muto"),
                keep_releases=self._get_parameter("storage_keep_releases", 2),
            )

            # Load download configuration
            download_config = DownloadConfig(
                retries=self._get_parameter("download_retries", 3),
                timeout_seconds=self._get_parameter("download_timeout_seconds", 60),
                backoff_seconds=self._get_parameter("download_backoff_seconds", 2.0),
            )

            # Load executor configuration
            executor_config = ExecutorConfig(
                start_grace_seconds=self._get_parameter("executor_start_grace_seconds", 10),
                stop_timeout_seconds=self._get_parameter("executor_stop_timeout_seconds", 10),
            )

            device_id = self._get_parameter("device_id", symphony_config.target_name)

            self._config = AgentConfig(
                device_id=device_id,
                mqtt=mqtt_config,
                topics=topic_config,
                symphony=symphony_config,
                storage=storage_config,
                downloads=download_config,
                executor=executor_config,
            )
            self._validate_config()

            self._node.get_logger().info("Configuration loaded successfully")
            return self._config

        except Exception as e:
            raise ConfigurationError(f"Failed to load configuration: {e}") from e

    def get_config(self) -> AgentConfig:
        """
        Get the current configuration.

        Returns:
            AgentConfig: The current configuration.

        Raises:
            ConfigurationError: If configuration has not been loaded.
        """
        if self._config is None:
            raise ConfigurationError("Configuration not loaded. Call load_config() first.")
        return self._config

    def _declare_parameters(self) -> None:
        """Declare all ROS parameters with their default values."""
        import rclpy

        parameters = [
            # MQTT
            ("host", "sandbox.composiv.ai"),
            ("port", 1883),
            ("keep_alive", 60),
            ("user", ""),
            ("password", ""),
            ("namespace", ""),
            ("prefix", "muto"),
            ("name", ""),
            # Topics
            ("stack_topic", "stack"),
            ("twin_topic", "twin"),
            ("agent_to_gateway_topic", "agent_to_gateway"),
            ("gateway_to_agent_topic", "gateway_to_agent"),
            ("agent_to_commands_topic", "agent_to_command"),
            ("commands_to_agent_topic", "command_to_agent"),
            ("thing_messages_topic", "thing_messages"),
            # Device
            ("device_id", "muto-device-001"),
            # Symphony
            ("symphony_enabled", False),
            ("symphony_host", "localhost"),
            ("symphony_port", 1883),
            ("symphony_keep_alive", 60),
            ("symphony_namespace", ""),
            ("symphony_prefix", "muto"),
            ("symphony_target_name", "muto-device-001"),
            ("symphony_topic_prefix", "symphony"),
            ("symphony_api_url", "http://localhost:8082/v1alpha2/"),
            ("symphony_user", "admin"),
            ("symphony_password", ""),
            ("symphony_name", "muto-device-001"),
            ("symphony_provider_name", "providers.target.mqtt"),
            ("symphony_broker_address", "tcp://localhost:1883"),
            ("symphony_client_id", "symphony"),
            ("symphony_request_topic", "coa-request"),
            ("symphony_response_topic", "coa-response"),
            ("symphony_timeout_seconds", 30),
            ("symphony_auto_register", False),
            # Storage
            ("storage_root_dir", "/var/lib/muto"),
            ("storage_keep_releases", 2),
            # Download
            ("download_retries", 3),
            ("download_timeout_seconds", 60),
            ("download_backoff_seconds", 2.0),
            # Executor
            ("executor_start_grace_seconds", 10),
            ("executor_stop_timeout_seconds", 10),
        ]

        for param_name, default_value in parameters:
            try:
                self._node.declare_parameter(param_name, default_value)
            except rclpy.exceptions.ParameterAlreadyDeclaredException:
                pass  # Parameter already declared

    def _get_parameter(self, name: str, default: Any) -> Any:
        """
        Get a parameter value safely.

        Args:
            name: Parameter name.
            default: Default value if parameter is not set.

        Returns:
            The parameter value.
        """
        try:
            value = self._node.get_parameter(name).value
            # Handle string-typed integers (e.g., "30" for timeout)
            if isinstance(default, int) and isinstance(value, str):
                return int(value)
            if isinstance(default, float) and isinstance(value, str):
                return float(value)
            return value
        except Exception:
            return default

    def _validate_config(self) -> None:
        """
        Validate the loaded configuration.

        Raises:
            ConfigurationError: If configuration is invalid.
        """
        if not self._config:
            raise ConfigurationError("Configuration is None")

        # Validate MQTT configuration
        if self._config.mqtt.port < 1 or self._config.mqtt.port > 65535:
            raise ConfigurationError(f"Invalid MQTT port: {self._config.mqtt.port}")

        if self._config.mqtt.keep_alive < 1:
            raise ConfigurationError(f"Invalid MQTT keep_alive: {self._config.mqtt.keep_alive}")

        # Validate required fields
        if not self._config.mqtt.host:
            raise ConfigurationError("MQTT host is required")


# Backwards compatibility alias
DeviceConfig = AgentConfig
