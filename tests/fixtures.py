"""Shared functions for tests."""

from typing import ChainMap

import json

from bgtransport.transport.base import BaseTransport

TEST_STRING = (
    "The quick brown fox jumped over the lazy dog."
    "The quick brown fox jumped over the lazy dog."
    "The quick brown fox jumped over the lazy dog."
)


def string_serializer(data: str) -> bytes:
    """Serialize a string to utf-8"""
    return data.encode("utf-8")


def chainmap_json_serializer(data: ChainMap) -> bytes:
    """Simplistic JSON serializer."""
    return json.dumps(dict(data)).encode("utf-8")


def dump_processor(data: str):
    """A processor that dumps the data."""
    return None


def noop_processor(data: str) -> str:
    """A processor that does nothing."""
    return data


def error_processor(data: str) -> str:
    """A processor that raises an error."""
    1 / 0
    return data


class DummyTransport(BaseTransport):
    """A transport for testing."""

    def __init__(self):
        super().__init__(chainmap_json_serializer)
        self._sent_items = []

    def send(self, data):
        """Add item to sent items."""
        self._sent_items.append(data)
