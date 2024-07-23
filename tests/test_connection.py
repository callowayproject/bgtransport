"""Test the connection module."""

import json
import time
from collections import ChainMap

import bgtransport
from bgtransport import connection
from tests.fixtures import DummyTransport


def dummy_function_one():
    """write data to a transaction."""
    cxn = connection.get_task("test")
    cxn.update(foo="foo", bar="bar")


def dummy_function_two():
    """write data to a transaction."""
    cxn = connection.get_task("test")
    cxn.update(bobby="tables", is_a_test=True)


def test_basic_connection(mocker):
    """Test basic connection"""
    transport = DummyTransport()
    cxn = connection.create_task("test", transport=transport)
    assert "test" in connection.REGISTRY._connections

    cxn.new_record(id="12345")
    dummy_function_one()
    dummy_function_two()

    assert cxn.get_record() == ChainMap(
        {
            "agent_name": "bgtransport",
            "agent_version": bgtransport.__version__,
            "id": "12345",
            "foo": "foo",
            "bar": "bar",
            "bobby": "tables",
            "is_a_test": True,
        }
    )

    cxn.send_record()
    time.sleep(0.5)
    assert len(transport._sent_items) == 1
    assert json.loads(transport._sent_items[0].decode("utf-8")) == {
        "agent_name": "bgtransport",
        "agent_version": bgtransport.__version__,
        "id": "12345",
        "foo": "foo",
        "bar": "bar",
        "bobby": "tables",
        "is_a_test": True,
    }
