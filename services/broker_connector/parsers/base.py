from abc import ABC, abstractmethod

from ..models import AlertData


class BaseParser(ABC):
    """Contract every broker parser must fulfill.

    Two-phase design keeps transport (Kafka bytes) separate from semantics
    (what the alert means), which lets tests inject dicts without needing
    a live Kafka stream.
    """

    @abstractmethod
    def decode(self, raw: bytes, key: bytes = None) -> dict | None:
        """Deserialize raw Kafka message bytes into a broker-specific dict.

        ``key`` carries the Avro schema on Fink topics — pass it through when available.
        Return None if the message cannot be decoded (log and skip upstream).
        """

    @abstractmethod
    def parse(self, topic: str, alert: dict) -> AlertData | None:
        """Map a broker-specific alert dict to a normalized AlertData.

        Return None if required fields are missing (log and skip upstream).
        """
