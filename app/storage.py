"""SQLite-backed event storage.

Concurrency model
-----------------
* All reads and writes go through a single global :class:`Store` guarded by a
  ``threading.RLock``; the HTTP layer always touches the store off the SSE
  streaming path, so the lock is never held while bytes are pushed to a
  client.
* Every batch is validated *before* any write and committed in one
  transaction, which makes the "all accepted or zero writes" guarantee
  atomic even when the process dies mid-request.

Idempotency / conflict rules
----------------------------
A batch is keyed by its ``eventKey`` values.  For every key that already has
a stored event on the same channel the stored ``(time, severity, message)``
triple must match the incoming one exactly:

* identical  -> replay: the already assigned global id is returned;
* different  -> the whole batch is rejected with HTTP 409 and nothing is
  written, including keys never seen before;
* unknown key -> a fresh monotonically increasing global id is assigned.
"""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from typing import Optional

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    channel     TEXT NOT NULL,
    event_key   TEXT NOT NULL,
    time        TEXT NOT NULL,
    severity    TEXT NOT NULL,
    message     TEXT NOT NULL,
    created_seq INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS channels (
    channel        TEXT PRIMARY KEY,
    seq            INTEGER NOT NULL DEFAULT 0,
    earliest_kept  INTEGER NOT NULL DEFAULT 1
);

-- One stored event per (channel, event_key).
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_channel_key
    ON events(channel, event_key);
-- Per-channel publish-order scans / retention trimming.
CREATE INDEX IF NOT EXISTS idx_events_channel_seq
    ON events(channel, created_seq);
"""


@dataclass(frozen=True)
class Event:
    id: int
    channel: str
    event_key: str
    time: str
    severity: str
    message: str
    created_seq: int


class BatchValidationError(ValueError):
    """The batch itself is malformed (e.g. a duplicated key within it)."""


class ConflictError(Exception):
    """A retried key carries different content than the stored event."""

    def __init__(self, *, event_key: str, stored: Event, incoming: dict):
        self.event_key = event_key
        self.stored = stored
        self.incoming = incoming
        super().__init__(
            f"conflict for eventKey {event_key!r} on channel "
            f"{stored.channel!r}: stored event {stored.id} differs from retry"
        )


class Store:
    def __init__(self, db_path: str, retention_limit: int):
        self.retention_limit = retention_limit
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            db_path,
            check_same_thread=False,
            isolation_level=None,  # explicit BEGIN/COMMIT
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------
    # Ingestion
    # ------------------------------------------------------------------
    def ingest_batch(
        self,
        channel: str,
        events: list[dict],
        on_commit=None,
    ) -> tuple[list[dict], list[Event]]:
        """Atomically accept (or fully reject) a batch of 1..50 events.

        Returns ``(results, new_events)`` where results is a list of
        ``{"eventKey", "id", "replayed"}`` in input order and ``new_events``
        are the freshly stored :class:`Event` objects (empty on a pure
        replay).  Raises :class:`ConflictError` on any key/content mismatch;
        in that case nothing is written.

        ``on_commit(new_events)`` runs after the COMMIT while still holding
        the serialization lock, so publishers fan events out in exactly the
        committed global-id order.
        """
        with self._lock:
            conn = self._conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                results, new_events = self._ingest_locked(channel, events)
            except Exception:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
            if on_commit is not None and new_events:
                on_commit(new_events)
            return results, new_events

    def _ingest_locked(
        self, channel: str, events: list[dict]
    ) -> tuple[list[dict], list[Event]]:
        conn = self._conn
        # Phase 1: validate against stored state. No writes happen until
        # every event has been classified, so a conflict rolls back even
        # the bookkeeping below.
        existing = {
            row["event_key"]: self._row_to_event(row)
            for row in conn.execute(
                "SELECT * FROM events WHERE channel = ? AND event_key IN (%s)"
                % ",".join("?" * len(events)),
                (channel, *(e["eventKey"] for e in events)),
            )
        }

        classifications: list[tuple[dict, Optional[Event]]] = []
        seen_in_batch: set[str] = set()
        for event in events:
            key = event["eventKey"]
            if key in seen_in_batch:
                # Duplicate keys inside one batch are an ambiguous retry:
                # reject the request itself rather than guessing which
                # payload the client meant.
                raise BatchValidationError(
                    f"eventKey {key!r} appears more than once in the batch"
                )
            seen_in_batch.add(key)
            classifications.append((event, existing.get(key)))

        for event, stored in classifications:
            if stored is not None and not self._same_content(stored, event):
                raise ConflictError(
                    event_key=stored.event_key,
                    stored=stored,
                    incoming=event,
                )

        # Phase 2: all clear -> write bookkeeping first.
        conn.execute(
            "INSERT INTO channels(channel) VALUES(?) "
            "ON CONFLICT(channel) DO NOTHING",
            (channel,),
        )
        row = conn.execute(
            "SELECT seq FROM channels WHERE channel = ?",
            (channel,),
        ).fetchone()
        seq = row["seq"]

        results: list[dict] = []
        new_events: list[Event] = []
        for event, stored in classifications:
            if stored is not None:
                results.append(
                    {
                        "eventKey": stored.event_key,
                        "id": stored.id,
                        "replayed": True,
                    }
                )
                continue

            cur = conn.execute(
                "INSERT INTO events(id, channel, event_key, time, "
                "severity, message, created_seq) "
                "VALUES (NULL, ?, ?, ?, ?, ?, ?)",
                (
                    channel,
                    event["eventKey"],
                    event["time"],
                    event["severity"],
                    event["message"],
                    seq,
                ),
            )
            new_id = cur.lastrowid
            seq += 1
            results.append(
                {
                    "eventKey": event["eventKey"],
                    "id": new_id,
                    "replayed": False,
                }
            )
            new_events.append(
                Event(
                    id=new_id,
                    channel=channel,
                    event_key=event["eventKey"],
                    time=event["time"],
                    severity=event["severity"],
                    message=event["message"],
                    created_seq=seq - 1,
                )
            )

        conn.execute(
            "UPDATE channels SET seq = ? WHERE channel = ?",
            (seq, channel),
        )

        # Phase 3: per-channel retention. Recompute the low-water mark and
        # drop everything below it. The global id space never shrinks.
        self._apply_retention(channel)
        return results, new_events

    @staticmethod
    def _same_content(stored: Event, incoming: dict) -> bool:
        return (
            stored.time == incoming["time"]
            and stored.severity == incoming["severity"]
            and stored.message == incoming["message"]
        )

    def _apply_retention(self, channel: str) -> None:
        conn = self._conn
        count = conn.execute(
            "SELECT COUNT(*) AS n FROM events WHERE channel = ?", (channel,)
        ).fetchone()["n"]
        if count <= self.retention_limit:
            return
        overflow = count - self.retention_limit
        # Oldest per-channel publish order first.
        conn.execute(
            """
            DELETE FROM events
             WHERE channel = ?
               AND id IN (
                   SELECT id FROM events
                    WHERE channel = ?
                    ORDER BY created_seq ASC, id ASC
                    LIMIT ?
               )
            """,
            (channel, channel, overflow),
        )
        row = conn.execute(
            "SELECT MIN(id) AS m FROM events WHERE channel = ?", (channel,)
        ).fetchone()
        earliest = row["m"]
        if earliest is not None:
            conn.execute(
                "UPDATE channels SET earliest_kept = ? WHERE channel = ?",
                (earliest, channel),
            )

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------
    def earliest_available_id(self, channel: str) -> Optional[int]:
        with self._lock:
            row = self._conn.execute(
                "SELECT MIN(id) AS m FROM events WHERE channel = ?", (channel,)
            ).fetchone()
            return row["m"]

    def channel_state(self, channel: str) -> tuple[bool, int, Optional[int]]:
        """Return (channel exists, next seq, earliest kept id)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT seq, earliest_kept FROM channels WHERE channel = ?",
                (channel,),
            ).fetchone()
            if row is None:
                return False, 0, None
            return True, row["seq"], row["earliest_kept"]

    def history_after(
        self, channel: str, after_id: int
    ) -> tuple[list[Event], Optional[int]]:
        """Stored events with id strictly greater than ``after_id``.

        Returns ``(events, earliest_available)``.  ``earliest_available`` is
        the caller's signal that ``after_id`` is too old: when it is greater
        than ``after_id + 1`` some retained ids are missing and the client
        must not be silently continued.
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events WHERE channel = ? AND id > ? "
                "ORDER BY id ASC",
                (channel, after_id),
            ).fetchall()
            earliest_row = self._conn.execute(
                "SELECT MIN(id) AS m FROM events WHERE channel = ?", (channel,)
            ).fetchone()
            return [self._row_to_event(r) for r in rows], earliest_row["m"]

    def list_events(self, channel: str) -> list[Event]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events WHERE channel = ? ORDER BY id ASC",
                (channel,),
            ).fetchall()
            return [self._row_to_event(r) for r in rows]

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> Event:
        return Event(
            id=row["id"],
            channel=row["channel"],
            event_key=row["event_key"],
            time=row["time"],
            severity=row["severity"],
            message=row["message"],
            created_seq=row["created_seq"],
        )
