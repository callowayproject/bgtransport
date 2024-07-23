"""A transport class that sends records to the console."""

import errno
import sys
import threading
from typing import Any, BinaryIO, Callable, Optional

import structlog

from bgtransport.serializer import Serializer
from bgtransport.transport.base import BaseTransport

logger = structlog.get_logger(__name__)


def until_not_interrupted(f: Callable[..., Any], *args: Any, **kw: Any) -> Any:
    """
    Retry until *f* succeeds or an exception that isn't caused by EINTR occurs.

    Args:
        f: A callable like a function.
        *args: Positional arguments for *f*.
        **kw: Keyword arguments for *f*.

    Returns:
        The result of the callable.

    Raises:
        OSError: If something goes wrong.
    """
    while True:
        try:
            return f(*args, **kw)
        except OSError as e:  # pragma: no branch
            if e.args[0] == errno.EINTR:
                continue
            logger.exception(e)
            raise


class ConsoleTransport(BaseTransport):
    """A transport class that sends records to the console."""

    def __init__(
        self,
        serializer: Serializer,
        processors: Optional[list] = None,
        stream: Optional[BinaryIO] = None,
    ):
        super().__init__(serializer, processors)
        self._file = stream or sys.stdout.buffer
        self._write = self._file.write
        self._flush = self._file.flush
        self._lock = threading.Lock()

    def send(self, data: bytes) -> None:
        """
        Send data to the console.

        Args:
            data: What to send
        """
        try:
            with self._lock:
                until_not_interrupted(self._write, data + b"\n")
                until_not_interrupted(self._flush)
        except Exception as e:  # pragma: no branch
            logger.exception(e)
