"""Test the compressed queue."""

import pytest

from bgtransport import compressed_queue


def test_compressed_queue_put_string():
    """A string should be acceptable."""
    q = compressed_queue.CompressedQueue()
    original = "The quick brown fox jumped over the lazy dog"
    q.put(original)
    result = q.get()
    assert result.decode("utf-8") == original


def test_compressed_queue_put_bytes():
    """Bytes should be acceptable."""
    q = compressed_queue.CompressedQueue()
    original = "The quick brown fox jumped over the lazy dog".encode("utf-8")
    q.put(original)
    result = q.get()
    assert result == original


def test_compressed_queue_put_other():
    """Putting in something other than bytes should raise an exception."""
    q = compressed_queue.CompressedQueue()
    with pytest.raises(ValueError):
        q.put(1)


def test_compressed_queue_size():
    """Test the size calculation."""
    q = compressed_queue.CompressedQueue()
    original = (
        "The quick brown fox jumped over the lazy dog"
        "The quick brown fox jumped over the lazy dog"
        "The quick brown fox jumped over the lazy dog"
    ).encode("utf-8")
    q.put(original)
    assert q.length == 1
    assert q.size < len(original)


def test_compressed_queue_bad_size():
    """Can't put in 0 or neegative sizes."""
    with pytest.raises(ValueError):
        compressed_queue.CompressedQueue(maxsize=0)
