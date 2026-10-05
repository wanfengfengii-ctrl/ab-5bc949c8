"""HTTP + SSE front end.

Routes
------
``GET  /health``                      liveness/readiness probe
``POST /events/{channel}``            atomic idempotent batch ingest
``GET  /streams/{channel}``           Server-Sent Events resume stream

The stream uses the global event id as the SSE ``id`` field and honors
``Last-Event-ID``: history strictly after the cursor is flushed first, then
live events are merged in.  Subscriptions are registered *before* history is
read and live events are skipped while ``id <= last delivered id``, so every
event appears exactly once regardless of publish timing.
"""

from __future__ import annotations

import json
import queue
import sys
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional
from urllib.parse import urlparse, parse_qs

from .broker import OVERFLOW, SHUTDOWN, Broker
from .config import Config
from .storage import BatchValidationError, ConflictError, Event, Store
from .validation import ValidationError, validate_batch, validate_channel


def _sse_payload(event: Event) -> str:
    data = {
        "id": event.id,
        "eventKey": event.event_key,
        "time": event.time,
        "severity": event.severity,
        "message": event.message,
    }
    return f"id: {event.id}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


class Handler(BaseHTTPRequestHandler):
    server_version = "InterlockEvents/1.0"
    protocol_version = "HTTP/1.1"
    broker: Broker
    store: Store
    config: Config

    # ------------------------------------------------------------------
    # plumbing
    # ------------------------------------------------------------------
    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
        sys.stderr.write(
            "%s - - [%s] %s\n"
            % (self.address_string(), self.log_date_time_string(), fmt % args)
        )

    def _send_json(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _send_error(self, status: int, code: str, message: str, **extra: Any):
        payload = {"error": code, "message": message, **extra}
        self._send_json(status, payload)

    def _channel_from_path(self, prefix: str) -> Optional[str]:
        path = urlparse(self.path).path
        if not path.startswith(prefix):
            return None
        channel = path[len(prefix):]
        if "/" in channel:
            return None
        return channel

    # ------------------------------------------------------------------
    # routing
    # ------------------------------------------------------------------
    def do_GET(self) -> None:  # noqa: N802 (stdlib naming)
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._handle_health()
            return
        channel = self._channel_from_path("/streams/")
        if channel is not None:
            self._handle_stream(channel, parse_qs(parsed.query))
            return
        self._send_error(404, "not_found", f"unknown path {parsed.path}")

    def do_POST(self) -> None:  # noqa: N802
        channel = self._channel_from_path("/events/")
        if channel is None:
            parsed = urlparse(self.path)
            self._send_error(404, "not_found", f"unknown path {parsed.path}")
            return
        self._handle_post_events(channel)

    # ------------------------------------------------------------------
    # endpoints
    # ------------------------------------------------------------------
    def _handle_health(self) -> None:
        try:
            self.store.earliest_available_id("__health__")
        except Exception:  # pragma: no cover - defensive
            self._send_error(503, "unhealthy", "storage is not available")
            return
        self._send_json(
            200,
            {
                "status": "ok",
                "retentionLimit": self.config.retention_limit,
                "heartbeatSeconds": self.config.heartbeat_seconds,
            },
        )

    def _handle_post_events(self, raw_channel: str) -> None:
        try:
            channel = validate_channel(raw_channel)
        except ValidationError as exc:
            self._send_error(400, "invalid_channel", str(exc))
            return

        length_header = self.headers.get("Content-Length")
        if length_header is None:
            self._send_error(411, "length_required", "Content-Length required")
            return
        try:
            length = int(length_header)
        except ValueError:
            self._send_error(400, "bad_request", "invalid Content-Length")
            return
        if length <= 0:
            self._send_error(400, "empty_body", "request body is empty")
            return
        if length > self.config.max_body_bytes:
            self._send_error(413, "payload_too_large", "batch exceeds size limit")
            return

        try:
            raw = self.rfile.read(length)
            payload = json.loads(raw.decode("utf-8"))
            events = validate_batch(payload)
        except UnicodeDecodeError:
            self._send_error(400, "bad_encoding", "body must be UTF-8 JSON")
            return
        except json.JSONDecodeError as exc:
            self._send_error(
                400,
                "invalid_json",
                f"body is not valid JSON: {exc.msg}",
                line=exc.lineno,
                column=exc.colno,
            )
            return
        except ValidationError as exc:
            self._send_error(400, "validation_failed", str(exc))
            return

        # Fan out from inside the store's serialization point (right after
        # COMMIT, while its lock is still held): broker puts are non-blocking
        # queue operations, so live delivery order always follows committed
        # global-id order even under concurrent publishers.
        def _on_commit(new_events: list[Event]) -> None:
            self.broker.publish(channel, new_events)

        try:
            results, _new = self.store.ingest_batch(
                channel, events, on_commit=_on_commit
            )
        except ConflictError as exc:
            # Zero writes happened; nothing is fanned out either.
            self._send_json(
                409,
                {
                    "error": "conflict",
                    "message": (
                        "batch rejected with no writes: retried eventKey "
                        f"{exc.event_key!r} carries different content"
                    ),
                    "channel": channel,
                    "conflicts": [
                        {
                            "eventKey": exc.event_key,
                            "storedId": exc.stored.id,
                            "stored": {
                                "time": exc.stored.time,
                                "severity": exc.stored.severity,
                                "message": exc.stored.message,
                            },
                            "incoming": {
                                "time": exc.incoming["time"],
                                "severity": exc.incoming["severity"],
                                "message": exc.incoming["message"],
                            },
                        }
                    ],
                },
            )
            return
        except BatchValidationError as exc:
            self._send_error(400, "validation_failed", str(exc))
            return

        replayed = sum(1 for r in results if r["replayed"])
        self._send_json(
            200,
            {
                "channel": channel,
                "accepted": len(results),
                "newEvents": len(results) - replayed,
                "replayed": replayed,
                "events": results,
            },
        )

    def _parse_cursor(self, query: dict[str, list[str]]) -> tuple[int, bool]:
        """Return ``(cursor, present)``.

        ``present`` is False when the client supplied no Last-Event-ID at
        all: a fresh console starts at the oldest retained event and must
        never receive 410.
        """
        raw = self.headers.get("Last-Event-ID")
        if raw is None and query.get("lastEventId"):
            raw = query["lastEventId"][0]
        if raw is None or raw == "":
            return 0, False
        try:
            value = int(raw)
        except ValueError:
            raise ValidationError(
                f"Last-Event-ID must be an integer id, got {raw!r}"
            )
        if value < 0:
            raise ValidationError("Last-Event-ID must not be negative")
        return value, True

    def _handle_stream(self, raw_channel: str, query: dict) -> None:
        try:
            channel = validate_channel(raw_channel)
        except ValidationError as exc:
            self._send_error(400, "invalid_channel", str(exc))
            return

        try:
            last_id, cursor_present = self._parse_cursor(query)
        except ValidationError as exc:
            self._send_error(400, "invalid_cursor", str(exc))
            return

        # Register before reading history so no commit can slip the gap.
        sub = self.broker.subscribe(channel)
        try:
            history, earliest = self.store.history_after(channel, last_id)

            # An explicit cursor strictly older than the first retained id
            # points at evicted history: force a re-snapshot (410) instead of
            # silently continuing with a hole. A client with no cursor is a
            # fresh console that simply starts at the oldest retained event.
            if (
                cursor_present
                and earliest is not None
                and last_id < earliest
            ):
                self._send_json(
                    410,
                    {
                        "error": "cursor_expired",
                        "message": (
                            "Last-Event-ID is older than the earliest "
                            "available event for this channel"
                        ),
                        "channel": channel,
                        "lastEventId": last_id,
                        "earliestAvailableId": earliest,
                    },
                )
                return

            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "close")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()
            # HTTP/1.1 without Content-Length: frame by connection close.
            self.close_connection = True

            delivered_max = last_id

            def write_raw(chunk: str) -> bool:
                try:
                    self.wfile.write(chunk.encode("utf-8"))
                    self.wfile.flush()
                    return True
                except (BrokenPipeError, ConnectionResetError, OSError):
                    return False

            # Replay banner helps clients log resume boundaries.
            if not write_raw(
                f": resume channel={channel} after={delivered_max} "
                f"replayed={len(history)}\n\n"
            ):
                return

            for event in history:
                if event.id <= delivered_max:
                    continue
                if not write_raw(_sse_payload(event)):
                    return
                delivered_max = event.id

            self._stream_live(sub, delivered_max, write_raw)
        finally:
            self.broker.unsubscribe(sub)

    def _stream_live(self, sub, delivered_max: int, write_raw) -> None:
        timeout = self.config.heartbeat_seconds
        while True:
            try:
                item = sub.queue.get(timeout=timeout)
            except queue.Empty:
                if not write_raw(f": heartbeat {int(time.time())}\n\n"):
                    return
                continue

            if item is SHUTDOWN:
                write_raw(": server shutting down\n\n")
                return
            if item is OVERFLOW:
                # Do not advance the id; reconnect replays the gap from
                # retention (or surfaces 410 if it also aged out).
                write_raw(
                    'event: stream-error\ndata: {"reason":"slow_consumer",'
                    '"action":"reconnect_with_last_event_id"}\n\n'
                )
                return

            event: Event = item
            if event.id <= delivered_max:
                # Already delivered from the history snapshot.
                continue
            if not write_raw(_sse_payload(event)):
                return
            delivered_max = event.id


class ThreadedServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def build_server(config: Config) -> tuple[ThreadedServer, Store, Broker]:
    store = Store(config.db_path, config.retention_limit)
    broker = Broker()

    class _BoundHandler(Handler):
        pass

    _BoundHandler.store = store
    _BoundHandler.broker = broker
    _BoundHandler.config = config

    server = ThreadedServer(("0.0.0.0", config.port), _BoundHandler)
    return server, store, broker


def serve(config: Config) -> None:
    import signal

    server, store, broker = build_server(config)

    def _stop(signum, frame):  # pragma: no cover - signal path
        broker.shutdown()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    try:
        sys.stderr.write(
            f"interlock event service listening on :{config.port} "
            f"(retention={config.retention_limit}, db={config.db_path})\n"
        )
        server.serve_forever(poll_interval=0.5)
    finally:
        broker.shutdown()
        server.server_close()
        store.close()
