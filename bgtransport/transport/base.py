"""Base transport class."""

import os
import queue
import threading
import timeit
from enum import Enum
from typing import Any, Optional

import structlog

from bgtransport.serializer import Serializer

logger = structlog.get_logger(__name__)


class ClosedTransportError(Exception):
    """Raised when the transport is closed."""

    pass


class TransportCommand(str, Enum):
    """
    Commands to the transport queue processor.
    """

    STORE = "STORE"
    """Store the data passed in."""

    CLOSE = "CLOSE"
    """Close the connection and clean up everything."""


class TransportState:
    """
    Is the service we should talk to up or down?
    """

    ONLINE = 1
    ERROR = 0

    def __init__(self):
        self.status = self.ONLINE
        self.last_check = None
        self.retry_number = -1

    def should_try(self) -> bool:
        """
        Should we try to send data to the backend?

        Returns:
            `True` if things are good.
        """
        if self.status == self.ONLINE:
            return True

        interval = min(self.retry_number, 6) ** 2

        return timeit.default_timer() - self.last_check > interval

    def set_fail(self) -> None:
        """Set the current state to an error."""
        self.status = self.ERROR
        self.retry_number += 1
        self.last_check = timeit.default_timer()

    def set_success(self) -> None:
        """Set the current state to success."""
        self.status = self.ONLINE
        self.last_check = None
        self.retry_number = -1

    def did_fail(self) -> bool:
        """Is the current state an error?"""
        return self.status == self.ERROR


class BaseTransport:
    """
    Base transport class.

    Process:

    1. queue() -> Put raw data or instructions into `_event_queue` for background processing
    3. _flush() ->
    """

    state: TransportState
    """The current health state of the transport destination."""

    _serializer: Serializer
    """The callable to serialize values to bytes."""

    _processors: list
    """A list of processors to modify data before serializing."""

    _pid: Optional[int]
    """The parent process id. Set by `start_thread()`."""

    _record_queue: queue.Queue
    """The raw data to serialize and process. Items are added by `queue()`."""

    def __init__(self, serializer: Serializer, processors: Optional[list] = None):
        self.state = TransportState()
        self._serializer = serializer
        self._processors = processors or []
        self._record_queue = queue.Queue(maxsize=10000)
        self._pid = None
        self._thread: Optional[threading.Thread] = None
        self._closed = False

    def start_thread(
        self, pid: Optional[int] = None, name: Optional[str] = None
    ) -> None:
        """
        Start the transport thread.

        Args:
            pid: The parent process id
            name: Name to use to name the thread
        """
        self._pid = pid or os.getpid()

        if not name:
            name = self.__class__.__name__

        if not self._thread and not self._closed:
            self.handle_fork()
            self._thread = threading.Thread(
                target=self._process_record_queue,
                name=f"{name} transport thread",
                daemon=True,
            )
            self._thread.pid = self._pid  # type: ignore[attr-defined]
            self._thread.start()

    def queue(
        self, data: Any, command: TransportCommand = TransportCommand.STORE
    ) -> None:
        """
        Queue the raw data and command to the event queue for background processing.

        Args:
            data: The data to store, or `None` if command is not STORE
            command: The command to the processor on what to do.

        Raises:
            ClosedTransportError: Raised if trying to queue a closed transport
        """
        if self._closed:
            raise ClosedTransportError("You can't queue data on closed transports.")

        if not self._thread:
            self.start_thread()

        try:
            self._record_queue.put_nowait((command, data))
        except queue.Full:
            logger.debug("Record dropped due to full event queue")

    @property
    def queue_has_data(self) -> bool:
        """Does the record queue contain data?"""
        return not self._record_queue.empty()

    def _process_record_queue(self) -> None:
        """
        Continuously process the record queue.

        This method runs in a background thread.

        Basic workflow:

        1. Pop an item off record queue
        2. Process the record
        3. Serialize the processed data
        4. Call the `send()` method to send it off
        """
        while True:
            command, data = self._record_queue.get()

            if data is not None:
                processed_data = self._process_record(data)
                if processed_data is not None:
                    try:
                        self.pre_send(processed_data)
                        self.send(self._serializer(processed_data))
                    except Exception:  # NOQA: BLE001
                        logger.warning(
                            "dropped record due to error while sending.", exc_info=True
                        )

            if command == TransportCommand.CLOSE:
                self.close()
                return

    def _process_record(self, data: Any) -> Any:
        """
        Run the data through processors.

        Args:
            data: The raw data to process

        Returns:
            `None` if the data should be ignored, or the processed data.
        """
        for processor in self._processors:
            try:
                data = processor(data)
                if not data:
                    logger.debug(
                        f"Dropped record due to processor {processor.__module__}.{processor.__name__}"
                    )
                    return None
            except Exception:  # NOQA: BLE001
                logger.warning(
                    f"Dropped record due to exception in processor {processor.__module__}.{processor.__name__}",
                    exc_info=True,
                )
                return None
        return data

    def pre_send(self, data: Any) -> None:
        """Helper method to look at the data before serializing and sending it."""
        pass

    def send(self, data: Any) -> None:
        """
        Send data to the server.

        Args:
            data: What to send
        """
        pass

    def close(self) -> None:
        """
        Clean up resources and close the connection.
        """
        self._closed = True

    def handle_fork(self) -> None:
        """
        Helper method to run code after a fork has been detected.
        """
        pass
