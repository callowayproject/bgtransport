"""Tesets the base transport classes."""

import time

from bgtransport.transport import base
from tests.fixtures import (
    TEST_STRING,
    dump_processor,
    error_processor,
    noop_processor,
    string_serializer,
)


def test_transport_state():
    """Test the basic functionality of the TransportState class."""
    ts = base.TransportState()
    assert ts.should_try()

    ts.set_fail()
    assert ts.status == ts.ERROR
    assert ts.did_fail()
    ts.retry_number = 6
    assert ts.should_try() is False

    ts.set_success()
    assert ts.should_try()


def test_basic_transport_functionality(mocker):
    """Test the basic functionality."""
    transport_send = mocker.patch("bgtransport.transport.base.BaseTransport.send")
    transport = base.BaseTransport(serializer=string_serializer)
    transport.start_thread()
    transport.queue(TEST_STRING)
    time.sleep(0.1)
    transport_send.assert_called_once()
    assert transport._closed is False
    transport.queue(None, base.TransportCommand.CLOSE)
    time.sleep(0.1)
    assert transport._closed


def test_processor_returns_none(mocker):
    transport_send = mocker.patch("bgtransport.transport.base.BaseTransport.send")
    transport = base.BaseTransport(
        serializer=string_serializer, processors=[dump_processor]
    )
    transport.start_thread()
    transport.queue(TEST_STRING)
    time.sleep(0.1)
    transport_send.assert_not_called()


def test_processor_raises_error(mocker):
    transport_send = mocker.patch("bgtransport.transport.base.BaseTransport.send")
    transport = base.BaseTransport(
        serializer=string_serializer, processors=[error_processor]
    )
    transport.start_thread()
    transport.queue(TEST_STRING)
    time.sleep(0.1)
    transport_send.assert_not_called()


def test_basic_processor(mocker):
    transport_send = mocker.patch("bgtransport.transport.base.BaseTransport.send")
    transport = base.BaseTransport(
        serializer=string_serializer, processors=[noop_processor]
    )
    transport.start_thread()
    transport.queue(TEST_STRING)
    time.sleep(0.1)
    transport_send.assert_called_once()
