import asyncio
from contextlib import contextmanager
from datetime import datetime
from typing import Callable, Generic, Optional, TypeVar


class Counter:
    """Counter for which the event of it reaching 0 can be asynchronously
    awaited."""

    def __init__(self):
        self._counter = 0
        self._zero = asyncio.Event()
        self._zero.set()

    def increment(self):
        """Increment the counter."""

        self._zero.clear()
        self._counter += 1

    def decrement(self):
        """Decrement the counter. If it reaches 0, the `zero` event is
        triggered. If it already is at 0, nothing happens."""

        if self._counter > 0:
            self._counter -= 1

        if self._counter == 0:
            self._zero.set()

    @contextmanager
    def bump(self):
        """Increment the counter upon entering the `with` block, decrement upon
        exiting."""

        self.increment()
        yield
        self.decrement()

    def clear(self):
        """Set the counter to zero. This triggers the `zero` event just like
        `decrement`."""

        self._counter = 0
        self._zero.set()

    async def until_zero(self):
        """Wait until the counter is at 0."""

        await self._zero.wait()

    def is_zero(self) -> bool:
        """Check whether the counter is 0."""

        return self._counter == 0


T = TypeVar("T")


class DelayedClear(Generic[T]):
    """A wrapper around an arbitrary value. The value can be accessed with the
    `get` context manager. The value can be cleared and later re-set.
    Asynchronously one can wait for the value to be set or to be no longer in
    use.

    The idea is that the value is some expensive async-shared resource that we
    only want to keep alive for a limited time if it is not in use. See
    `clear_routine`."""

    def __init__(self, value: Optional[T] = None):
        self._value_set = asyncio.Event()
        if value is not None:
            self.set(value)
        self._use_counter = Counter()
        self._last_release = datetime.now()

    @contextmanager
    def get(self, or_set: Callable[[], T]):
        """Get the contained value. If it isn't set, `or_set` is called to set
        it."""

        if not hasattr(self, "_value"):
            self.set(or_set())

        try:
            with self._use_counter.bump():
                yield self._value
        finally:
            if self._use_counter.is_zero():
                self._last_release = datetime.now()

    def set(self, value: T):
        """Set the value, independent of whether it is currently in use."""


        self._value = value
        self._value_set.set()

    def since_release(self) -> float:
        """Seconds since the value was last released by a `get` call. Does *not*
        take into account whether the value is in use right now (see `in_use`
        for that)."""

        return (datetime.now() - self._last_release).total_seconds()

    def unused(self) -> bool:
        """Whether the value is currently unused, i.e. there is no active `get`
        context."""

        return self._use_counter.is_zero()

    async def until_set(self):
        """Wait until the value is set."""

        await self._value_set.wait()

    async def until_unused(self):
        """Wait until the value is unused."""

        await self._use_counter.until_zero()

    def clear(self) -> bool:
        """Clear the value if not in use. Returns `False` if in use, otherwise
        `True`, even if the value is already clear and nothing is done."""

        if self.unused():
            if hasattr(self, "_value"):
                del self._value
                self._value_set.clear()
            return True
        else:
            return False

