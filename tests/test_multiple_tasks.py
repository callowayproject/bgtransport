"""Test multiple tasks work independently."""

import asyncio
import json
import time
from collections import ChainMap

import pytest

import bgtransport
from bgtransport import connection
from tests.fixtures import DummyTransport


def dummy_function_one():
    """write data to a transaction."""
    cxn = connection.get_task("test1")
    cxn.update(foo="foo", bar="bar")


def dummy_function_two():
    """write data to a transaction."""
    cxn = connection.get_task("test2")
    cxn.update(bobby="tables", is_a_test=True)


def test_multiple_tasks_are_independent():
    """Each task should store stuff independently."""

    transport1 = DummyTransport()
    transport2 = DummyTransport()
    task1 = connection.create_task("test1", transport=transport1)
    task2 = connection.create_task("test2", transport=transport2)
    assert "test1" in connection.REGISTRY._connections
    assert "test2" in connection.REGISTRY._connections

    task1.new_record(id="12345")
    task2.new_record(id="67890")
    dummy_function_one()
    dummy_function_two()

    assert task1.get_record() == ChainMap(
        {
            "agent_name": "bgtransport",
            "agent_version": bgtransport.__version__,
            "id": "12345",
            "foo": "foo",
            "bar": "bar",
        }
    )

    assert task2.get_record() == ChainMap(
        {
            "agent_name": "bgtransport",
            "agent_version": bgtransport.__version__,
            "id": "67890",
            "bobby": "tables",
            "is_a_test": True,
        }
    )

    task1.send_record()
    task2.send_record()
    time.sleep(0.5)
    assert len(transport1._sent_items) == 1
    assert len(transport2._sent_items) == 1
    assert json.loads(transport1._sent_items[0].decode("utf-8")) == {
        "agent_name": "bgtransport",
        "agent_version": bgtransport.__version__,
        "id": "12345",
        "foo": "foo",
        "bar": "bar",
    }
    assert json.loads(transport2._sent_items[0].decode("utf-8")) == {
        "agent_name": "bgtransport",
        "agent_version": bgtransport.__version__,
        "id": "67890",
        "bobby": "tables",
        "is_a_test": True,
    }
