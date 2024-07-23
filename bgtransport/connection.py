"""Connection management interface."""

import os
import threading
from collections import ChainMap
from typing import Optional

import structlog

from bgtransport import __version__
from bgtransport.context import BGTransportContext, random_id
from bgtransport.transport.base import BaseTransport


class Task:
    """A named connection to a background transporter."""

    def __init__(
        self, name: str, transport: BaseTransport, metadata: Optional[dict] = None
    ):
        cls = self.__class__
        self.name = name
        self.logger = structlog.get_logger(f"{cls.__module__}.{cls.__name__}.{name}")
        self.error_logger = structlog.get_logger("bgtransport.error")
        self.pid: Optional[int] = None
        self._transport = transport
        self._thread_starter_lock = threading.Lock()
        self._metadata = metadata or {}
        self._metadata["agent_name"] = "bgtransport"
        self._metadata["agent_version"] = __version__
        self.execution_context = BGTransportContext(self.name)

    def start_transport_thread(self) -> None:
        """Tell the transport to kick off its background thread."""
        current_pid = os.getpid()

        if self.pid == current_pid:
            return

        with self._thread_starter_lock:
            self.logger.debug(f"Detected PID change from {self.pid} to {current_pid}")
            self.logger.debug("Starting transport thread")
            self._transport.start_thread(pid=current_pid, name=self.name)
            self.pid = current_pid

    def new_record(self, **kwargs) -> None:
        """
        Start a record buffer.

        All calls to `update` will add to the record buffer. A call to `send_record`
        will queue the record for transport.

        Args:
            **kwargs: Initial key/value pairs to add to the record
        """
        self.start_transport_thread()
        self.execution_context.clear_transaction()
        metadata = self._metadata.copy()
        metadata.update(kwargs)
        if "id" not in metadata:
            metadata["id"] = random_id()
        self.execution_context.update_transaction(**metadata)

    def update(self, **kwargs) -> None:
        """
        Write key/value pairs to the record buffer.

        Args:
            **kwargs: key/value pairs to add to the record
        """
        self.start_transport_thread()
        self.execution_context.update_transaction(**kwargs)

    def get_record(self) -> ChainMap:
        """
        Get the record buffer.
        """
        self.start_transport_thread()
        return self.execution_context.get_transaction()

    def send_record(self) -> None:
        """
        Send the record buffer.

        The buffer is serialized and queued for transport.
        """
        self.start_transport_thread()
        trx_data = self.execution_context.get_transaction(clear=True)
        self._transport.queue(trx_data)


class TaskRegistry:
    """A registry of named tasks."""

    def __init__(self):
        self._connections = {}

    def __getitem__(self, key: str) -> Task:
        """Get a named task from the registry."""
        return self._connections[key]

    def __setitem__(self, key: str, value: Task) -> None:
        """Set a named task in the registry."""
        self._connections[key] = value


REGISTRY = TaskRegistry()


def create_task(
    name: str, transport: BaseTransport, metadata: Optional[dict] = None
) -> Task:
    """
    Create a named connection for background transport.

    Args:
        name: The name of the connection.
        transport: The method of transport.
        metadata: Metadata to include with every record.

    Returns:
        The created task.
    """
    REGISTRY[name] = Task(name=name, transport=transport, metadata=metadata)
    return REGISTRY[name]


def get_task(name: str) -> Task:
    """
    Attempt to get the named connecetion.

    Raises:
        KeyError: If the name does not exist.

    Args:
        name: The name of the previously created connection.

    Returns:
        The connection.
    """
    return REGISTRY[name]
