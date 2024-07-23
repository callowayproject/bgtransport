"""Tests for the kafka transport module."""

from typing import Optional, Union

import re
from time import sleep

from json import dumps as json_serialize

from bgtransport.transport import kafka
from bgtransport.transport.base import TransportCommand


class FakeKafkaClient:
    def __init__(self):
        self.topic = None
        self.key = None
        self.value = None

    def flush(self, timeout: Optional[float] = 1.0):
        pass

    def produce(
        self,
        topic,
        value: Union[str, bytes, None] = None,
        key: Union[str, bytes, None] = None,
        *args,
        **kwargs,
    ):
        self.topic = topic
        self.key = key
        self.value = value


def test_default_key_generator():
    """Default key generator returns the ``id`` value or a random value."""
    assert kafka.default_key_generator({"id": 1234}) == 1234

    output = kafka.default_key_generator({"uid": 1234})
    assert re.fullmatch(r"[a-f0-9]{32}", output)


def test_no_key_generator():
    """The no key generator returns returns ``None``."""
    assert kafka.no_key_generator({"id": 1234}) is None


def test_keygetter():
    """The keygetter returns a specific key or default value."""
    get_customer = kafka.KeyGetter("customer_id", "00000000")
    assert get_customer({"customer_id": "1234567"}) == "1234567"
    assert get_customer({}) == "00000000"


def test_kafka_transport():
    """KafkaTransport works as expected."""
    fake_client = FakeKafkaClient()
    data = {"id": "1234567", "foo": "bar"}
    transport = kafka.KafkaTransport(json_serialize, fake_client, "mytopic")
    transport.queue(data)
    transport.queue(None, TransportCommand.CLOSE)
    sleep(0.1)

    assert fake_client.key == data["id"]
    assert fake_client.value == json_serialize(data)
    assert fake_client.topic == "mytopic"
