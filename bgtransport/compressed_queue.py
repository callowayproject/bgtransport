"""A queue that compresses the data in it."""

import lzma
from collections import deque
from typing import Optional, Union


class CompressedQueue:
    """
    A queue-like structure that compresses data put into it.

    It uses a `deque` so it can check the total size of the items.
    """

    def __init__(self, maxsize: Optional[int] = None):
        if maxsize is not None and maxsize < 1:
            raise ValueError("maxsize must be a positive integer.")
        self._deque: deque = deque(maxlen=maxsize)

    @property
    def size(self) -> int:
        """The size of all the items in bytes."""
        return sum(len(item) for item in self._deque)

    @property
    def length(self) -> int:
        """The number of items in the queue."""
        return len(self._deque)

    def reset(self) -> None:
        """Clear out the queue."""
        self._deque.clear()

    def put(self, value: Union[bytes, str]) -> None:
        """
        Compress and put item into the queue.

        Args:
            value: A serialized representation of the value

        Raises:
            ValueError: If the value is not a string or bytes value.
        """
        if isinstance(value, str):
            value = value.encode("utf-8")
        elif not isinstance(value, bytes):
            raise ValueError("value must be a string or bytes.")
        self._deque.append(lzma.compress(value))

    def get(self, decompress: bool = True) -> bytes:
        """
        Get and decompress the next value in the queue.

        Setting the `decompress` argument to `False` is useful if the queue
        processor wants to use the compressed version.

        Args:
            decompress: If `False`, don't decompress the value.

        Returns:
            The (de)compressed data, depending on the value of `decompress`
        """
        value = self._deque.popleft()
        return lzma.decompress(value) if decompress else value
