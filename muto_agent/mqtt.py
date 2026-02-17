#
# Copyright (c) 2023 Composiv.ai
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

# Standard library imports
import json
import signal
import threading

# Third-party imports
import rclpy
from muto_msgs.msg import Gateway, GraphDrift, GraphEvent, MutoActionMeta, Thing, ThingHeaders
from paho.mqtt.client import MQTTMessage
from rclpy.qos import QoSDurabilityPolicy, QoSProfile, QoSReliabilityPolicy
from std_msgs.msg import String as StringMsg

from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from .config import AgentConfig, ConfigurationManager
from .exceptions import ConfigurationError, ConnectionError

# Local imports
from .interfaces import BaseNode
from .mqtt_manager import DittoMessageHandler, MQTTConnectionManager


class MQTT(BaseNode):
    """
    Enhanced MQTT Gateway with improved modularity and robustness.

    This class provides a robust MQTT gateway that handles communication
    between the Muto Agent system and external MQTT brokers using the
    Ditto protocol.

    Features:
    - Modular MQTT connection management
    - Robust error handling and reconnection
    - Proper resource management
    - Configuration management
    - Comprehensive logging
    """

    def __init__(self):
        """Initialize the MQTT Gateway."""
        super().__init__("mqtt_gateway")

        self._config_manager: ConfigurationManager | None = None
        self._config: AgentConfig | None = None
        self._mqtt_manager: MQTTConnectionManager | None = None
        self._message_handler: DittoMessageHandler | None = None

        # ROS publishers and subscribers
        self._pub_agent = None
        self._sub_agent = None
        self._pub_thing = None

        # Graph state bridge subscribers
        self._sub_graph_state = None
        self._sub_graph_drift = None
        self._sub_graph_events = None

    def _do_initialize(self) -> None:
        """Initialize the MQTT gateway components."""
        try:
            # Initialize configuration
            self._config_manager = ConfigurationManager(self)
            self._config = self._config_manager.load_config()

            # Setup ROS communication
            self._setup_ros_communication()

            # Initialize message handler
            self._message_handler = DittoMessageHandler(
                self._config.mqtt.namespace,
                self._config.mqtt.name,
                self._send_to_agent,
                self._publish_thing_message,
                self._publish_error_message,
                self.get_logger(),
            )

            # Initialize MQTT connection
            self._mqtt_manager = MQTTConnectionManager(self, self._config.mqtt, self._handle_mqtt_message)

            # Establish MQTT connection
            if not self._mqtt_manager.connect():
                raise ConnectionError("Failed to establish MQTT connection")

        except Exception as e:
            self.get_logger().error(f"Failed to initialize MQTT Gateway: {e}")
            raise ConfigurationError(f"Gateway initialization failed: {e}") from e

    def _setup_ros_communication(self) -> None:
        """Setup ROS publishers and subscribers."""
        topics = self._config.topics

        self._pub_agent = self.create_publisher(Gateway, topics.gateway_to_agent_topic, 10)
        self._sub_agent = self.create_subscription(Gateway, topics.agent_to_gateway_topic, self._agent_msg_callback, 10)
        self._pub_thing = self.create_publisher(Thing, topics.thing_messages_topic, 10)

        # Graph state bridge: forward ROS graph topics to MQTT for dashboard
        self._setup_graph_state_bridge()

    def _handle_mqtt_message(self, message: MQTTMessage) -> None:
        """
        Handle incoming MQTT messages.

        Args:
            message: The MQTT message to handle.
        """
        try:
            if self._message_handler:
                self._message_handler.handle_message(message)
            else:
                self.get_logger().error("Message handler not initialized")
        except Exception as e:
            self.get_logger().error(f"Failed to handle MQTT message: {e}")

    def _send_to_agent(self, thing_message: dict, meta: MutoActionMeta) -> None:
        """
        Send message to agent via ROS.

        Args:
            thing_message: The parsed thing message.
            meta: Message metadata.
        """
        try:
            msg = Gateway()
            msg.topic = thing_message.get("topic", "")
            msg.payload = json.dumps(thing_message)
            msg.meta = meta

            if self._pub_agent:
                self._pub_agent.publish(msg)
                self.get_logger().debug("Message sent to agent")
            else:
                self.get_logger().error("Agent publisher not available")

        except Exception as e:
            self.get_logger().error(f"Failed to send message to agent: {e}")

    def _agent_msg_callback(self, data: Gateway) -> None:
        """
        Handle messages from agent and publish to MQTT.

        Args:
            data: Gateway message from agent.
        """
        try:
            payload = data.payload
            response_topic = data.meta.response_topic
            correlation_data = data.meta.correlation_data

            if not self._mqtt_manager or not self._mqtt_manager.is_connected():
                self.get_logger().error("MQTT not connected, cannot publish response")
                return

            # Create MQTT properties
            properties = Properties(PacketTypes.PUBLISH)
            if correlation_data:
                properties.CorrelationData = correlation_data.encode()

            # Publish to MQTT
            success = self._mqtt_manager.publish(response_topic, payload, properties)
            if success:
                self.get_logger().debug(f"Response published to {response_topic}")
            else:
                self.get_logger().error(f"Failed to publish response to {response_topic}")

        except Exception as e:
            self.get_logger().error(f"Failed to handle agent message: {e}")

    def _setup_graph_state_bridge(self) -> None:
        """Subscribe to ROS graph topics and forward them to MQTT."""
        try:
            # /muto/graph_state_json is best_effort (matches daemon publisher)
            json_qos = QoSProfile(
                depth=1,
                reliability=QoSReliabilityPolicy.BEST_EFFORT,
                durability=QoSDurabilityPolicy.VOLATILE,
            )
            self._sub_graph_state = self.create_subscription(
                StringMsg, "/muto/graph_state_json",
                self._on_graph_state_json, json_qos,
            )

            # /muto/graph_drift is reliable + transient_local (matches daemon)
            drift_qos = QoSProfile(
                depth=1,
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
            )
            self._sub_graph_drift = self.create_subscription(
                GraphDrift, "/muto/graph_drift",
                self._on_graph_drift, drift_qos,
            )

            # /muto/graph_events is reliable, volatile
            event_qos = QoSProfile(
                depth=50,
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
            )
            self._sub_graph_events = self.create_subscription(
                GraphEvent, "/muto/graph_events",
                self._on_graph_event, event_qos,
            )

            self.get_logger().info("Graph state MQTT bridge initialized")

        except Exception as e:
            self.get_logger().warning(f"Graph state bridge setup failed: {e}")

    def _graph_mqtt_topic(self, suffix: str) -> str:
        """Build MQTT topic for graph state messages."""
        return (
            f"{self._config.mqtt.prefix}/"
            f"{self._config.mqtt.namespace}:{self._config.mqtt.name}/"
            f"outbox/graph/{suffix}"
        )

    def _publish_graph_mqtt(self, suffix: str, payload: str) -> None:
        """Publish a graph state message to MQTT."""
        if not self._mqtt_manager or not self._mqtt_manager.is_connected():
            return
        topic = self._graph_mqtt_topic(suffix)
        self._mqtt_manager.publish(topic, payload)

    def _on_graph_state_json(self, msg: StringMsg) -> None:
        """Forward graph state JSON from ROS to MQTT."""
        try:
            self._publish_graph_mqtt("state", msg.data)
        except Exception as e:
            self.get_logger().error(f"Failed to forward graph state: {e}")

    def _on_graph_drift(self, msg: GraphDrift) -> None:
        """Serialize GraphDrift and forward to MQTT."""
        try:
            payload = json.dumps({
                "missing_nodes": list(msg.missing_nodes),
                "unexpected_nodes": list(msg.unexpected_nodes),
                "parameter_drifts": list(msg.parameter_drifts),
                "timestamp": {
                    "sec": msg.timestamp.sec,
                    "nanosec": msg.timestamp.nanosec,
                },
            })
            self._publish_graph_mqtt("drift", payload)
        except Exception as e:
            self.get_logger().error(f"Failed to forward graph drift: {e}")

    def _on_graph_event(self, msg: GraphEvent) -> None:
        """Serialize GraphEvent and forward to MQTT."""
        try:
            payload = json.dumps({
                "timestamp": {
                    "sec": msg.timestamp.sec,
                    "nanosec": msg.timestamp.nanosec,
                },
                "event_type": msg.event_type,
                "stack_name": msg.stack_name,
                "node_name": msg.node_name,
                "node_namespace": msg.node_namespace,
                "details": msg.details,
            })
            self._publish_graph_mqtt("events", payload)
        except Exception as e:
            self.get_logger().error(f"Failed to forward graph event: {e}")

    def _publish_thing_message(self, payload: dict, channel: str, action: str, meta: MutoActionMeta) -> None:
        """
        Publish Ditto thing message via ROS.

        Args:
            payload: Ditto Protocol message.
            channel: Message channel (e.g., "live", "twin").
            action: Action type for the message.
            meta: Message metadata.
        """
        try:
            # Create thing headers
            thing_headers = ThingHeaders()
            headers = payload.get("headers", {})

            if headers:
                thing_headers.reply_to = headers.get("reply-to", "")
                thing_headers.correlation_id = headers.get("correlation-id", "")
                thing_headers.ditto_originator = headers.get("ditto-originator", "")
                thing_headers.response_required = headers.get("response-required", False)
                thing_headers.content_type = headers.get("content-type", "")

            # Create thing message
            msg_thing = Thing()
            msg_thing.topic = payload.get("topic", "")
            msg_thing.headers = thing_headers
            msg_thing.path = payload.get("path", "")
            msg_thing.value = json.dumps(payload.get("value", ""))
            msg_thing.channel = channel
            msg_thing.action = action
            msg_thing.meta = meta

            if self._pub_thing:
                self._pub_thing.publish(msg_thing)
                self.get_logger().debug(f"Thing message published for channel: {channel}, action: {action}")
            else:
                self.get_logger().error("Thing publisher not available")

        except Exception as e:
            self.get_logger().error(f"Failed to publish thing message: {e}")

    def _publish_error_message(
        self,
        meta: MutoActionMeta,
        status: int = 400,
        error: str = "",
        message: str = "",
        description: str = "",
    ) -> None:
        """
        Publish Ditto error message via MQTT.

        Args:
            meta: Message metadata.
            status: HTTP status code.
            error: Error code identifier.
            message: Human readable error message.
            description: Additional error description.
        """
        try:
            if not self._config or not self._mqtt_manager:
                self.get_logger().error("Cannot publish error: configuration or MQTT manager not initialized")
                return

            # Create error payload
            error_payload = {
                "topic": f"{self._config.mqtt.namespace}/{self._config.mqtt.name}/things/twin/errors",
                "headers": {"correlation-id": meta.correlation_data},
                "path": "/",
                "value": {
                    "status": status,
                    "error": error,
                    "message": message,
                    "description": description,
                },
                "status": status,
            }

            payload_json = json.dumps(error_payload)
            response_topic = f"{self._config.mqtt.prefix}/{self._config.mqtt.namespace}:{self._config.mqtt.name}"

            # Create MQTT properties
            properties = Properties(PacketTypes.PUBLISH)
            if meta.correlation_data:
                properties.CorrelationData = meta.correlation_data.encode()

            # Publish error message
            success = self._mqtt_manager.publish(response_topic, payload_json, properties)
            if success:
                self.get_logger().debug(f"Error message published: {error}")
            else:
                self.get_logger().error(f"Failed to publish error message: {error}")

        except Exception as e:
            self.get_logger().error(f"Failed to publish error message: {e}")

    def _do_cleanup(self) -> None:
        """Clean up MQTT gateway resources."""
        try:
            # Disconnect MQTT
            if self._mqtt_manager:
                self._mqtt_manager.disconnect()

            # Clean up ROS publishers/subscribers
            if self._sub_agent:
                self.destroy_subscription(self._sub_agent)
                self._sub_agent = None

            if self._pub_agent:
                self.destroy_publisher(self._pub_agent)
                self._pub_agent = None

            if self._pub_thing:
                self.destroy_publisher(self._pub_thing)
                self._pub_thing = None

            # Clean up graph state bridge subscriptions
            for sub_name in ("_sub_graph_state", "_sub_graph_drift", "_sub_graph_events"):
                sub = getattr(self, sub_name, None)
                if sub:
                    self.destroy_subscription(sub)
                    setattr(self, sub_name, None)

            self.get_logger().info("MQTT Gateway cleanup completed")

        except Exception as e:
            self.get_logger().error(f"Error during MQTT gateway cleanup: {e}")

    def is_mqtt_connected(self) -> bool:
        """
        Check if MQTT is connected.

        Returns:
            True if MQTT is connected, False otherwise.
        """
        return self._mqtt_manager is not None and self._mqtt_manager.is_connected()


def main():
    """Main entry point for the Muto MQTT."""
    provider = None
    shutdown_requested = threading.Event()

    def signal_handler(signum, frame):
        """Handle shutdown signals gracefully."""
        print(f"Received signal {signum}, initiating graceful shutdown...")
        shutdown_requested.set()
        if provider is not None:
            provider._shutdown_event.set()

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        rclpy.init()
        provider = MQTT()
        provider.initialize()

        provider.get_logger().info("Muto MQTT started successfully")

        # Custom spin loop to handle shutdown gracefully
        while rclpy.ok() and not shutdown_requested.is_set():
            try:
                rclpy.spin_once(provider, timeout_sec=1.0)
            except KeyboardInterrupt:
                break

    except KeyboardInterrupt:
        print("Muto MQTT interrupted by user")
    except Exception as e:
        print(f"Failed to start Muto MQTT: {e}")

    finally:
        # Cleanup provider if it was created
        if provider is not None:
            try:
                print("Cleaning up Muto MQTT...")
                provider.cleanup()
            except Exception as e:
                print(f"Error during provider cleanup: {e}")

        # Only shutdown ROS2 if it's still initialized and we haven't already shut it down
        try:
            if rclpy.ok():
                print("Shutting down ROS2...")
                rclpy.shutdown()
        except Exception as e:
            print(f"Error during ROS2 shutdown (this may be normal): {e}")


if __name__ == "__main__":
    exit(main())
