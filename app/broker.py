"""In-process per-channel fan-out from publishers to SSE subscribers.

A subscriber is created *before* the subscribing request reads history from
the store.  Every event committed afterwards therefore lands in the queue,
and the SSE handler de-duplicates against the history snapshot by id, which
closes the "event committed between history read and stream open" gap.

Queues are bounded: a slow console cannot grow memory without limit.  If a
queue ever overflows the subscriber is handed an :data:`OVERFLOW` sentinel
once; the SSE handler then tells the client to reconnect, and the standard
``Last-Event-ID`` resume path re-reads whatever retention still holds.
"""

from __future__ import annotations

import queue
import threading
from typing import Optional, Union

from .storage import Event

# Sentinel pushed once when a subscriber cannot keep up.
OVERFLOW = object()

# Sentinel pushed to every subscriber on shutdown so streams terminate.
SHUTDOWN = object()

StreamItem = Union[Event, object]


class _Subscription:
    __slots__ = ("channel", "queue", "overflowed")

    def __init__(self, channel: str, max_size: int):
        self.channel = channel
        self.queue: "queue.Queue[StreamItem]" = queue.Queue(maxsize=max_size)
        self.overflowed = False


class Broker:
    def __init__(self, queue_size: int = 512):
        self._queue_size = queue_size
        self._lock = threading.Lock()
        self._subs: dict[str, set[_Subscription]] = {}

    def subscribe(self, channel: str) -> _Subscription:
        sub = _Subscription(channel, self._queue_size)
        with self._lock:
            self._subs.setdefault(channel, set()).add(sub)
        return sub

    def unsubscribe(self, sub: _Subscription) -> None:
        with self._lock:
            subs = self._subs.get(sub.channel)
            if subs:
                subs.discard(sub)
                if not subs:
                    self._subs.pop(sub.channel, None)

    def publish(self, channel: str, events: list[Event]) -> None:
        """Fan events out. Must be called only after the DB commit succeeds."""
        if not events:
            return
        with self._lock:
            targets = list(self._subs.get(channel, ()))
        for sub in targets:
            for event in events:
                self._offer(sub, event)

    @staticmethod
    def _offer(sub: _Subscription, item: StreamItem) -> None:
        try:
            sub.queue.put_nowait(item)
        except queue.Full:
            # Drop the newest event for this slow consumer and signal once;
            # its Last-Event-ID resume will recover the gap from storage.
            with sub.queue.mutex:
                if not sub.overflowed:
                    sub.overflowed = True
                    try:
                        sub.queue.put_nowait(OVERFLOW)
                    except queue.Full:
                        try:
                            sub.queue.get_nowait()
                        except queue.Empty:
                            pass
                        sub.queue.put_nowait(OVERFLOW)

    def shutdown(self) -> None:
        with self._lock:
            targets = [s for subs in self._subs.values() for s in subs]
        for sub in targets:
            sub.queue.put(SHUTDOWN)

    def subscriber_count(self, channel: Optional[str] = None) -> int:
        with self._lock:
            if channel is not None:
                return len(self._subs.get(channel, ()))
            return sum(len(s) for s in self._subs.values())
