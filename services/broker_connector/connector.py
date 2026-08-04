"""Generic Kafka → SkyPortal connector."""

import time

from confluent_kafka import Consumer, KafkaError

from .parsers.base import BaseParser
from .sink import SkyPortalSink


class KafkaConnector:
    """Consume a set of Kafka topics and push parsed alerts to SkyPortal.

    One instance = one logical use-case. Two connectors on the same broker
    must have different ``group_id`` values so their offsets are independent
    and they can each process the full stream.
    """

    def __init__(
        self,
        name: str,
        broker: dict,
        topics: list,
        parser: BaseParser,
        sink: SkyPortalSink,
        poll_timeout: float = 5.0,
        log=None,
    ):
        self.name = name
        self.topics = topics
        self.parser = parser
        self.sink = sink
        self.poll_timeout = poll_timeout
        self.log = log or print

        kafka_conf = {
            "bootstrap.servers": broker["servers"],
            "group.id": broker["group_id"],
            "auto.offset.reset": broker.get("auto_offset_reset", "earliest"),
            "enable.auto.commit": True,
        }

        username = broker.get("username")
        password = broker.get("password")
        if username and password:
            kafka_conf.update(
                {
                    "security.protocol": broker.get("security_protocol", "SASL_SSL"),
                    "sasl.mechanism": broker.get("sasl_mechanism", "SCRAM-SHA-512"),
                    "sasl.username": username,
                    "sasl.password": password,
                }
            )

        self.consumer = Consumer(kafka_conf)
        self.consumer.subscribe(topics)
        self.log(
            f"[{self.name}] Subscribed to {topics} "
            f"on {broker['servers']} (group={broker['group_id']})"
        )

    def _poll_once(self):
        msg = self.consumer.poll(self.poll_timeout)
        if msg is None:
            self.log(f"[{self.name}] poll timeout — no message (waiting...)")
            return None, None
        if msg.error():
            code = msg.error().code()
            if code == KafkaError._PARTITION_EOF:
                self.log(
                    f"[{self.name}] reached end of partition (waiting for new messages)"
                )
            else:
                self.log(f"[{self.name}] Kafka error: {msg.error()}")
            return None, None
        self.log(
            f"[{self.name}] received message: topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()} "
            f"size={len(msg.value()) if msg.value() else 0}B"
        )
        alert = self.parser.decode(msg.value(), key=msg.key())
        if alert is None:
            self.log(f"[{self.name}] could not decode message at offset={msg.offset()}")
        return msg.topic(), alert

    def run(self):
        """Blocking poll loop. Call from a dedicated thread or process."""
        self.sink.init()
        self.log(f"[{self.name}] Starting poll loop")
        try:
            while True:
                topic, raw = self._poll_once()
                if raw is None:
                    continue
                alert_data = self.parser.parse(topic, raw)
                if alert_data is None:
                    self.log(
                        f"[{self.name}] Skipped alert on {topic} (parser returned None)"
                    )
                    continue
                self.sink.push(alert_data)
        except KeyboardInterrupt:
            self.log(f"[{self.name}] Interrupted")
        finally:
            self.consumer.close()
            self.log(f"[{self.name}] Consumer closed")
