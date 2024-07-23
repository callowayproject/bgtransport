"""Manage current state."""

import collections
import random
from contextvars import ContextVar


def random_id() -> str:
    """Return a 128-bit hex string."""
    return f"{random.getrandbits(128):032x}"


class BGTransportContext:
    """Stores and manages the global state."""

    def __init__(self, task_name: str):
        self.ctx_var_name = f"{task_name}_transaction_data"
        self._data: ContextVar = ContextVar(self.ctx_var_name)
        # This is stored in a list to back propagate the context:
        # see https://discuss.python.org/t/back-propagation-of-contextvar-changes-from-worker-threads/15928/16
        self._data.set([collections.ChainMap()])

    @property
    def transaction_data(self) -> collections.ChainMap:
        """
        Return the first item of the context variable.

        The actual value is stored in a list to back propagate the context:
        see https://discuss.python.org/t/back-propagation-of-contextvar-changes-from-worker-threads/15928/16

        Returns:
            The value that the caller _really_ wants.
        """
        return self._data.get()[0]

    @transaction_data.setter
    def transaction_data(self, value: collections.ChainMap) -> None:
        """
        Set the first item of the context variable.

        The actual value is stored in a list to back propagate the context:
        see https://discuss.python.org/t/back-propagation-of-contextvar-changes-from-worker-threads/15928/16

        Args:
            value: The new value for the context variable
        """
        cvar = self._data.get()
        cvar[0] = value

    def update_transaction(self, **kwargs) -> None:
        """
        Update the transaction buffer.

        Args:
            **kwargs: The key-value pairs to update
        """
        self.transaction_data = self.transaction_data.new_child(kwargs)

    def clear_transaction(self) -> None:
        """
        Set the transaction buffer to a new ChainMap.
        """
        self.transaction_data = collections.ChainMap()

    def get_transaction(self, clear: bool = False) -> collections.ChainMap:
        """
        Get the current transaction buffer.

        Args:
            clear: Clear the transaction after returning the transaction buffer

        Returns:
            The current transaction buffer
        """
        trx_data = self.transaction_data
        if clear:
            self.clear_transaction()
        return trx_data
