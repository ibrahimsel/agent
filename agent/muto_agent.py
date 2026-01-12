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
Main Muto Agent module providing centralized message routing and coordination.

This module implements the core Muto Agent that acts as a message router
between different components of the system including gateways, composers,
and command processors.

The agent supports two modes:
- ROS mode: Full ROS 2 integration with message handlers
- Standalone mode: Non-ROS mode for Symphony deployments
"""

from __future__ import annotations

import logging
import os
import signal
import threading
from typing import Optional, Tuple, TYPE_CHECKING

from .config import AgentConfig, ConfigurationManager
from .exceptions import ConfigurationError


# Try to import ROS modules - they're optional for standalone mode
try:
    import rclpy
    from std_msgs.msg import String
    from muto_msgs.msg import Gateway, MutoAction
    from .interfaces import BaseNode
    from .topic_parser import MutoTopicParser
    from .message_handlers import (
        GatewayMessageHandler,
        ComposerMessageHandler,
        CommandMessageHandler
    )
    ROS_AVAILABLE = True
except ImportError:
    ROS_AVAILABLE = False
    BaseNode = object  # Fallback for standalone mode


class _LogNode:
    """Simple logger wrapper for non-ROS mode."""
    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def get_logger(self) -> logging.Logger:
        return self._logger


class MutoAgent(BaseNode if ROS_AVAILABLE else object):
    """
    Main Muto Agent class that coordinates message routing between components.

    The MutoAgent acts as a central hub for message routing, handling communication
    between gateways, composers, and command processors. It uses a modular design
    with separate message handlers for different message types.

    The agent supports two operational modes:
    - ROS mode: Full ROS 2 integration with message handlers and topics
    - Standalone mode: Non-ROS mode for Symphony deployments only

    Features:
    - Centralized configuration management
    - Robust error handling with specific exception types
    - Modular message handling through dedicated handlers
    - Proper resource management and cleanup
    - Comprehensive logging
    - Symphony deployment integration (both modes)
    """

    def __init__(self, ros_mode: bool = True, config_path: Optional[str] = None):
        """
        Initialize the Muto Agent.

        Args:
            ros_mode: If True, initialize as ROS 2 node. If False, run standalone.
            config_path: Optional path to JSON config file (for standalone mode).
        """
        self._ros_mode = ros_mode and ROS_AVAILABLE
        self._config_path = config_path
        self._logger: Optional[logging.Logger] = None

        if self._ros_mode:
            super().__init__("muto_agent")
        else:
            # Setup standalone logging
            logging.basicConfig(
                level=os.environ.get("MUTO_LOG_LEVEL", "INFO").upper(),
                format="%(asctime)s %(levelname)s %(name)s: %(message)s",
            )
            self._logger = logging.getLogger("muto.agent")

        self._config_manager: Optional[ConfigurationManager] = None
        self._config: Optional[AgentConfig] = None
        self._topic_parser = None

        # Message handlers (ROS mode only)
        self._gateway_handler = None
        self._composer_handler = None
        self._command_handler = None

        # Publishers and subscribers (ROS mode only)
        self._pub_dict = {}
        self._sub_dict = {}

        # Symphony components (both modes)
        self._deployment_manager = None
        self._device_provider = None
        self._symphony_broker = None

        # Shutdown event for standalone mode
        self._shutdown_event = threading.Event()

    def get_logger(self):
        """Get the logger instance."""
        if self._ros_mode:
            return super().get_logger()
        return self._logger

    def _do_initialize(self) -> None:
        """Initialize the agent components."""
        try:
            # Initialize configuration
            if self._ros_mode:
                self._config_manager = ConfigurationManager(self)
            else:
                self._config_manager = ConfigurationManager(
                    node=None, config_path=self._config_path
                )
            self._config = self._config_manager.load_config()

            # Initialize Symphony/deployment components if enabled
            if self._config.symphony.enabled:
                self._initialize_symphony_components()

            # Initialize ROS-specific components
            if self._ros_mode:
                self._initialize_ros_components()

        except Exception as e:
            self.get_logger().error(f"Failed to initialize MutoAgent: {e}")
            raise ConfigurationError(f"Agent initialization failed: {e}") from e

    def _initialize_symphony_components(self) -> None:
        """Initialize Symphony deployment components."""
        from .deployment_manager import DeploymentManager
        from .symphony.device_provider import MutoDeviceProvider
        from .symphony.symphony_broker import MQTTBroker

        self.get_logger().info("Initializing Symphony deployment components")

        self._deployment_manager = DeploymentManager(self._config, self.get_logger())
        self._device_provider = MutoDeviceProvider(
            self._config, self._deployment_manager, self.get_logger()
        )

        # Create a log node wrapper for the broker
        if self._ros_mode:
            node = self
        else:
            node = _LogNode(self._logger)

        self._symphony_broker = MQTTBroker(
            plugin=self._device_provider,
            node=node,
            config=self._config
        )

        # Bootstrap existing stacks
        self._bootstrap_existing_stacks()

    def _bootstrap_existing_stacks(self) -> None:
        """Bootstrap any existing stacks on startup."""
        if not self._deployment_manager:
            return

        stacks_root = os.path.join(self._config.storage.root_dir, "stacks")
        if not os.path.isdir(stacks_root):
            return

        for entry in os.listdir(stacks_root):
            stack_path = os.path.join(stacks_root, entry)
            if not os.path.isdir(stack_path):
                continue
            self._deployment_manager.cleanup_incomplete_installations(entry)
            self._deployment_manager.restart_current_if_needed(entry)

    def _initialize_ros_components(self) -> None:
        """Initialize ROS-specific components (message handlers, topics)."""
        # Initialize topic parser
        self._topic_parser = MutoTopicParser(self.get_logger())

        # Initialize message handlers
        self._gateway_handler = GatewayMessageHandler(
            self, self._topic_parser, self._config.topics
        )
        self._composer_handler = ComposerMessageHandler(
            self, self._config.topics
        )
        self._command_handler = CommandMessageHandler(
            self, self._config.topics
        )

        # Setup ROS communication
        self._setup_ros_communication()

    def _setup_ros_communication(self) -> None:
        """Setup ROS publishers and subscribers."""
        topics = self._config.topics

        # Setup publishers
        self._pub_dict['gateway'] = self.create_publisher(
            Gateway, topics.agent_to_gateway_topic, 10
        )
        self._pub_dict['stack'] = self.create_publisher(
            MutoAction, topics.stack_topic, 10
        )
        self._pub_dict['commands'] = self.create_publisher(
            MutoAction, topics.agent_to_commands_topic, 10
        )

        # Setup subscribers
        self._sub_dict['gateway'] = self.create_subscription(
            Gateway, topics.gateway_to_agent_topic, self._gateway_msg_callback, 10
        )
        self._sub_dict['stack'] = self.create_subscription(
            String, topics.twin_topic, self._composer_msg_callback, 10
        )
        self._sub_dict['commands'] = self.create_subscription(
            MutoAction, topics.commands_to_agent_topic, self._commands_msg_callback, 10
        )

    def _gateway_msg_callback(self, data) -> None:
        """Callback function for gateway subscriber."""
        try:
            if self._gateway_handler:
                self._gateway_handler.handle_message(data)
            else:
                self.get_logger().error("Gateway handler not initialized")
        except Exception as e:
            self.get_logger().error(f"Failed to process gateway message: {e}")

    def _composer_msg_callback(self, data) -> None:
        """Callback function for composer subscriber."""
        try:
            if self._composer_handler:
                self._composer_handler.handle_message(data)
            else:
                self.get_logger().debug("Composer handler not initialized")
        except Exception as e:
            self.get_logger().error(f"Failed to process composer message: {e}")

    def _commands_msg_callback(self, data) -> None:
        """Callback function for commands subscriber."""
        try:
            if self._command_handler:
                self._command_handler.handle_message(data)
            else:
                self.get_logger().error("Command handler not initialized")
        except Exception as e:
            self.get_logger().error(f"Failed to process command message: {e}")

    def _do_cleanup(self) -> None:
        """Clean up agent resources."""
        try:
            # Clean up Symphony components
            if self._device_provider:
                self._device_provider.cleanup()
            if self._symphony_broker:
                self._symphony_broker.stop()

            # Clean up ROS components
            if self._ros_mode:
                for sub in self._sub_dict.values():
                    if sub:
                        self.destroy_subscription(sub)
                self._sub_dict.clear()

                for pub in self._pub_dict.values():
                    if pub:
                        self.destroy_publisher(pub)
                self._pub_dict.clear()

            self.get_logger().info("Agent cleanup completed")

        except Exception as e:
            self.get_logger().error(f"Error during cleanup: {e}")

    def connect_symphony(self) -> None:
        """Connect to Symphony MQTT broker."""
        if self._symphony_broker:
            self._symphony_broker.connect()

    def start_standalone(self) -> None:
        """
        Start the agent in standalone mode (non-ROS).

        This method blocks until shutdown is requested.
        """
        if self._ros_mode:
            raise RuntimeError("Cannot start standalone when in ROS mode")

        self.get_logger().info("Starting Muto Agent in standalone mode")

        # Initialize
        self._do_initialize()

        if not self._config.symphony.enabled:
            self.get_logger().error("Symphony integration disabled; exiting")
            return

        # Connect to Symphony MQTT
        self.connect_symphony()

        self.get_logger().info("Muto Agent started successfully (standalone mode)")

        # Wait for shutdown
        self._shutdown_event.wait()

        # Cleanup
        self._do_cleanup()

    def request_shutdown(self) -> None:
        """Request agent shutdown (for standalone mode)."""
        self._shutdown_event.set()

    def get_topic_parser(self):
        """Get the topic parser instance."""
        return self._topic_parser

    def get_config(self) -> Optional[AgentConfig]:
        """Get the agent configuration."""
        return self._config

    def parse_topic(self, topic: str) -> Tuple[Optional[str], Optional[str]]:
        """Parse topic using the topic parser."""
        if self._topic_parser:
            return self._topic_parser.parse_topic(topic)
        return None, None

    def is_ready(self) -> bool:
        """Check if the agent is fully initialized and ready."""
        if self._ros_mode:
            return (
                self._config is not None
                and self._topic_parser is not None
                and self._gateway_handler is not None
                and self._command_handler is not None
                and len(self._pub_dict) > 0
                and len(self._sub_dict) > 0
            )
        else:
            # Standalone mode - just check config and symphony
            return (
                self._config is not None
                and (not self._config.symphony.enabled or self._symphony_broker is not None)
            )


def main():
    """Main entry point for the Muto Agent (ROS mode)."""
    if not ROS_AVAILABLE:
        print("ROS 2 is not available. Use 'main_standalone' for non-ROS mode.")
        return

    agent = None
    shutdown_requested = threading.Event()

    def signal_handler(signum, frame):
        """Handle shutdown signals gracefully."""
        print(f"Received signal {signum}, initiating graceful shutdown...")
        shutdown_requested.set()

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        rclpy.init()
        agent = MutoAgent(ros_mode=True)
        agent.initialize()

        if agent.is_ready():
            agent.get_logger().info("Muto Agent started successfully")

            # Connect to Symphony if enabled
            if agent._config and agent._config.symphony.enabled:
                agent.connect_symphony()

            # Custom spin loop to handle shutdown gracefully
            while rclpy.ok() and not shutdown_requested.is_set():
                try:
                    rclpy.spin_once(agent, timeout_sec=1.0)
                except KeyboardInterrupt:
                    break
        else:
            agent.get_logger().error("Muto Agent failed to initialize properly")

    except KeyboardInterrupt:
        print("Muto Agent interrupted by user")
    except Exception as e:
        print(f"Failed to start Muto Agent: {e}")

    finally:
        # Cleanup agent if it was created
        if agent is not None:
            try:
                print("Cleaning up Muto Agent...")
                agent.cleanup()
            except Exception as e:
                print(f"Error during agent cleanup: {e}")

        # Only shutdown ROS2 if it's still initialized
        try:
            if rclpy.ok():
                print("Shutting down ROS2...")
                rclpy.shutdown()
        except Exception as e:
            print(f"Error during ROS2 shutdown (this may be normal): {e}")


if __name__ == "__main__":
    main()
