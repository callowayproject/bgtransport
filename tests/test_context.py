"""Test the execution context."""

from collections import ChainMap

from bgtransport import context


def test_random_id():
    """random_id returns a random string."""
    val1 = context.random_id()
    val2 = context.random_id()
    assert len(val1) == len(val2) == 32
    assert val1 != val2


def test_initial_context_is_empty():
    """When creating a context, the transaction should be a ChainMap."""
    val = context.BGTransportContext("test").get_transaction()
    assert isinstance(val, ChainMap)
    assert val.maps == [{}]


def test_update_transaction():
    """Multiple updates to a transaction are stored."""
    ctx = context.BGTransportContext("test")
    update_1 = {"a": 1, "b": 2, "c": 3}
    update_2 = {"c": 1, "d": 2, "e": 3}
    merged = {}
    merged.update(update_1)
    merged.update(update_2)

    # First update
    ctx.update_transaction(**update_1)
    assert ctx.get_transaction() == update_1

    # Second update
    ctx.update_transaction(**update_2)
    assert ctx.get_transaction() == merged

    # Check for 2 maps in the object
    assert ctx.get_transaction().maps == [update_2, update_1, {}]


def test_clear_transaction():
    """Clearing a transaction should end up with a clean slate."""
    ctx = context.BGTransportContext("test")
    ctx.update_transaction(**{"a": 1, "b": 2, "c": 3})
    val = ctx.get_transaction(clear=True)
    assert val == {"a": 1, "b": 2, "c": 3}
    new_val = ctx.get_transaction()
    assert new_val == {}
