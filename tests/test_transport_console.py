"""Tests the ConsoleTransport class."""

import io

from bgtransport.transport import console
from bgtransport.serializer import json_serialize


def test_console_transport():
    """Console transport should write data to a buffer."""
    stream = io.BytesIO()
    transport = console.ConsoleTransport(json_serialize, stream=stream)
    test_data = json_serialize({"id": 1234, "foo": "bar"})
    transport.send(test_data)
    output = stream.getvalue()
    assert output == test_data + b"\n"
