"""Test helpers: tiny HTTP client and a streaming SSE client (stdlib only)."""

from __future__ import annotations

import http.client
import json
import os
import queue
import threading
import time
from urllib.parse import urlsplit


def base_url() -> str:
    return os.environ.get("EVENT_SERVICE_URL", "http://127.0.0.1:8080")


def _split(url: str):
    parts = urlsplit(url)
    return parts.hostname or "127.0.0.1", parts.port or 80


def http_request(
    method: str,
    path: str,
    body=None,
    headers: dict | None = None,
    url: str | None = None,
):
    """Perform a simple request; return (status, headers, raw_bytes)."""
    host, port = _split(url or base_url())
    conn = http.client.HTTPConnection(host, port, timeout=15)
    hdrs = dict(headers or {})
    payload = None
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        hdrs.setdefault("Content-Type", "application/json")
    conn.request(method, path, body=payload, headers=hdrs)
    resp = conn.getresponse()
    raw = resp.read()
    status = resp.status
    resp_headers = {k.lower(): v for k, v in resp.getheaders()}
    conn.close()
    return status, resp_headers, raw


def http_json(method: str, path: str, body=None, headers=None):
    status, resp_headers, raw = http_request(method, path, body, headers)
    try:
        parsed = json.loads(raw.decode("utf-8")) if raw else None
    except json.JSONDecodeError:
        parsed = None
    return status, resp_headers, parsed


def post_batch(channel: str, events, headers=None):
    return http_json("POST", f"/events/{channel}", events, headers)


def make_event(key: str, **overrides) -> dict:
    event = {
        "eventKey": key,
        "time": "2026-10-05T12:00:00Z",
        "severity": "alarm",
        "message": f"event {key}",
    }
    event.update(overrides)
    return event


class SSEClient(threading.Thread):
    """Minimal SSE reader. Frames and comments land on ``items`` as dicts:

    * ``{"type": "event", "id": int, "event": str|None, "data": obj}``
    * ``{"type": "comment", "value": str}``
    """

    def __init__(self, path: str, headers: dict | None = None, url: str | None = None):
        super().__init__(daemon=True)
        host, port = _split(url or base_url())
        self._host = host
        self._port = port
        self._path = path
        self._headers = {"Accept": "text/event-stream", **(headers or {})}
        self.items: "queue.Queue[dict]" = queue.Queue()
        self.banner_seen = threading.Event()
        self.status: int | None = None
        self.resp_headers: dict = {}
        self.error_body: bytes | None = None
        self._conn = None

    def run(self) -> None:
        conn = http.client.HTTPConnection(self._host, self._port, timeout=30)
        self._conn = conn
        conn.request("GET", self._path, headers=self._headers)
        resp = conn.getresponse()
        self.status = resp.status
        self.resp_headers = {k.lower(): v for k, v in resp.getheaders()}
        if resp.status != 200:
            self.error_body = resp.read()
            conn.close()
            return

        frame_id: str | None = None
        frame_event: str | None = None
        data_lines: list[str] = []

        def flush():
            nonlocal frame_id, frame_event, data_lines
            if data_lines or frame_id is not None:
                raw_data = "\n".join(data_lines)
                try:
                    parsed = json.loads(raw_data) if raw_data else None
                except json.JSONDecodeError:
                    parsed = {"raw": raw_data}
                self.items.put(
                    {
                        "type": "event",
                        "id": int(frame_id) if frame_id is not None else None,
                        "event": frame_event,
                        "data": parsed,
                    }
                )
            frame_id, frame_event, data_lines = None, None, []

        try:
            while True:
                line = resp.fp.readline()
                if line == b"":
                    break
                line = line.decode("utf-8").rstrip("\r\n")
                if line == "":
                    flush()
                elif line.startswith(":"):
                    value = line[1:].lstrip()
                    if value.startswith("resume"):
                        self.banner_seen.set()
                    self.items.put({"type": "comment", "value": value})
                elif line.startswith("id:"):
                    frame_id = line[3:].strip()
                elif line.startswith("event:"):
                    frame_event = line[6:].strip()
                elif line.startswith("data:"):
                    data_lines.append(line[5:].lstrip())
        except (ConnectionResetError, http.client.HTTPException):
            pass
        finally:
            flush()
            conn.close()

    def next_item(self, timeout: float = 5.0):
        return self.items.get(timeout=timeout)

    def next_event(self, timeout: float = 5.0) -> dict:
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            item = self.items.get(timeout=max(0.01, remaining))
            if item["type"] == "event":
                return item

    def wait_for_banner(self, timeout: float = 5.0) -> None:
        """Block until the resume comment (written right after history).

        Safe to call after :meth:`next_event`: those calls skip past the
        comment but still flip the banner flag via the reader thread.
        """
        if self.banner_seen.is_set():
            return
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            item = self.items.get(timeout=deadline - time.monotonic())
            if item["type"] == "comment" and item["value"].startswith("resume"):
                return
        raise AssertionError("stream did not send resume banner in time")

    def close(self) -> None:
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception:
            pass
