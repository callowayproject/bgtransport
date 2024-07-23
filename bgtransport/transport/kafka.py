"""
Kafka transport.

Example:
    While this example uses the confluent_kafka.Producer class, any class that
    implements the `KafkaClient` protocol will work.

    ::

        from confluent_kafka import Producer
        from bgtransport.transport.kafka import KafkaTransport, KeyGetter
        from bgtransport.serializer import json_serialize
        from bgtransport import create_task

        def delivery_report(err, msg):
            if err is not None:
                print(f"Message delivery failed: {err}")
            else:
                print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

        kafka_client = Producer({'bootstrap.servers': 'mybroker1,mybroker2'})
        produce_kwargs = {"callback": delivery_report}
        transport = KafkaTransport(
            serializer=json_serialize,
            kafka_client=kafka_client,
            topic="mytopic",
            key_generator=KeyGetter("uid"),
            flush_timeout=0.5,
            produce_kwargs=produce_kwargs,
        )

        create_task("mytopic-sender", transport)
"""

from typing import Any, Callable, Optional, Protocol, Union

from bgtransport.context import random_id
from bgtransport.serializer import Serializer
from bgtransport.transport.base import BaseTransport

OptBytesOrStr = Union[bytes, str, None]


class KafkaClient(Protocol):
    """Definition of expectations of a Kafka client."""

    def flush(self, timeout: Optional[float] = 1.0) -> None:
        """Wait for all messages in the Producer queue to be delivered."""
        ...

    def produce(
        self,
        topic: str,
        value: OptBytesOrStr = None,
        key: OptBytesOrStr = None,
        *args,
        **kwargs,
    ) -> None:
        """Produce message to topic."""
        ...

    def poll(self, timeout: float) -> int:
        """Polls the producer for events and calls the corresponding callbacks (if registered)."""
        ...


def default_key_generator(data: dict) -> OptBytesOrStr:
    """A function that returns the `id` value of a dictionary."""
    return data.get("id", random_id())


def no_key_generator(data: dict) -> None:
    """Always returns `None`."""
    return None


class KeyGetter:
    """
    Creates a callable that will return the value of a key in a dictionary.

    Example:
        To get the "customer_id" key::

            >>> get_customer = KeyGetter("customer_id", "00000000")
            >>> get_customer({"customer_id": "1234567"})
            "1234567"
            >>> get_customer({})
            "00000000"
    """

    def __init__(self, key: str, default: Optional[str] = None):
        self.key = key
        self.default = default

    def __call__(self, data: dict) -> Optional[str]:
        """Return the value of a key in data."""
        return data.get(self.key, self.default)


class KafkaTransport(BaseTransport):
    """A Kafka transport."""

    topic: str
    """The topic to write the messages to."""

    kafka_client: KafkaClient
    """A class that follows the KafkaClient protocol."""

    flush_timeout: float
    """The maximum time to block in seconds."""

    produce_kwargs: dict
    """A dictionary of parameters to pass to the `produce` method."""

    key_generator: Callable[[dict], Union[str]]
    """A callable that will return the key to use for the data being sent."""

    def __init__(
        self,
        serializer: Serializer,
        kafka_client: KafkaClient,
        topic: str,
        key_generator: Optional[Callable[[Any], str]] = None,
        flush_timeout: Optional[float] = 1.0,
        produce_kwargs: Optional[dict] = None,
        processors: Optional[list] = None,
    ):
        super().__init__(serializer, processors)
        self.kafka_client = kafka_client
        self.topic = topic
        self.key_generator = key_generator or default_key_generator
        self.flush_timeout = flush_timeout or 1.0
        self.produce_kwargs = produce_kwargs or {}
        self._current_key: Optional[str] = None

    def pre_send(self, data: Any) -> None:
        """Capture the key from the raw data."""
        self._current_key = self.key_generator(data)

    def send(self, data: Union[None, str, bytes]) -> None:
        """Send data to Kafka."""
        self.kafka_client.produce(
            topic=self.topic, value=data, key=self._current_key, **self.produce_kwargs
        )
        self.kafka_client.poll(0)

        self._current_key = None

    def close(self) -> None:
        """Flushes the Kafka buffer."""
        super().close()
        self.kafka_client.flush(self.flush_timeout)
