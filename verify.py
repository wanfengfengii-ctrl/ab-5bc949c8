"""One-shot verification entry point.

Waits for the service to become healthy, then runs four independently
summarized groups and exits with a bitmask of the groups that failed:

    bit 0 (1)  code tests         (unit + black-box unittest suite)
    bit 1 (2)  build and publish  (byte-compile + batch publish/id/replay/409)
    bit 2 (4)  resume             (Last-Event-ID history+live continuation)
    bit 3 (8)  expired cursor     (410 + earliestAvailableId smoke)

Usage:
    EVENT_SERVICE_URL=http://host:port python verify.py
Inside docker compose the verify service sets that URL automatically.
"""

from __future__ import annotations

import compileall
import json
import os
import sys
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
TESTS_DIR = REPO_ROOT / "tests"
APP_DIR = REPO_ROOT / "app"

FAIL_CODE_TESTS = 1
FAIL_BUILD_PUBLISH = 2
FAIL_RESUME = 4
FAIL_EXPIRED_CURSOR = 8

GROUP_NAMES = {
    FAIL_CODE_TESTS: "code tests",
    FAIL_BUILD_PUBLISH: "build and publish",
    FAIL_RESUME: "resume",
    FAIL_EXPIRED_CURSOR: "expired cursor smoke",
}


def _base_url() -> str:
    return os.environ.get(
        "EVENT_SERVICE_URL",
        f"http://127.0.0.1:{os.environ.get('APP_PORT', '8080')}",
    ).rstrip("/")


def _request(method: str, path: str, body=None, headers=None, timeout=15):
    url = _base_url() + path
    data = None
    hdrs = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        hdrs["Content-Type"] = "application/json"
    if headers:
        hdrs.update(headers)
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, dict(resp.headers), raw
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers), exc.read()


def _json(method, path, body=None, headers=None, timeout=15):
    status, resp_headers, raw = _request(
        method, path, body, headers, timeout=timeout
    )
    try:
        return status, resp_headers, json.loads(raw)
    except json.JSONDecodeError:
        return status, resp_headers, None


def wait_for_healthy(deadline_seconds: float = 60.0) -> bool:
    print(f"[verify] waiting for service health at {_base_url()}/health ...")
    deadline = time.monotonic() + deadline_seconds
    while time.monotonic() < deadline:
        try:
            status, _, body = _json("GET", "/health", timeout=3)
            if status == 200 and isinstance(body, dict) and body.get("status") == "ok":
                print(
                    f"[verify] service healthy "
                    f"(retentionLimit={body.get('retentionLimit')}, "
                    f"heartbeatSeconds={body.get('heartbeatSeconds')})"
                )
                return True
        except (urllib.error.URLError, ConnectionError, OSError):
            pass
        time.sleep(0.25)
    print("[verify] ERROR: service did not become healthy in time")
    return False


def group_build() -> bool:
    print("\n[verify] == build: byte-compiling application ==")
    ok = compileall.compile_dir(str(APP_DIR), quiet=1, maxlevels=10)
    if ok:
        # Importability check (schema/migrations execute on open).
        try:
            sys.path.insert(0, str(REPO_ROOT))
            from app.config import load_config  # noqa: F401
            from app.server import build_server  # noqa: F401
            from app.storage import Store  # noqa: F401
            print("[verify] build: compile + import OK")
        except Exception as exc:
            print(f"[verify] build: import FAILED: {exc!r}")
            return False
    else:
        print("[verify] build: compileall reported syntax errors")
    return bool(ok)


def _event(key, **overrides) -> dict:
    base = {
        "eventKey": key,
        "time": "2026-10-05T00:00:00Z",
        "severity": "alarm",
        "message": f"verify {key}",
    }
    base.update(overrides)
    return base


def group_publish() -> tuple[bool, dict]:
    print("\n[verify] == publish: atomic batches, ids, replay, 409 == ")
    ctx: dict = {}
    import uuid

    ch = f"verify-pub-{uuid.uuid4().hex[:10]}"

    status, _, body = _json(
        "POST", f"/events/{ch}", [_event("p1"), _event("p2")]
    )
    if status != 200 or body.get("newEvents") != 2:
        print(f"[verify] publish FAILED: {status} {body}")
        return False, ctx
    ids = [e["id"] for e in body["events"]]
    if ids != sorted(ids) or len(set(ids)) != 2:
        print(f"[verify] publish FAILED: ids not unique/increasing: {ids}")
        return False, ctx

    status, _, body = _json("POST", f"/events/{ch}", [_event("p3")])
    if status != 200 or body["events"][0]["id"] <= ids[-1]:
        print("[verify] publish FAILED: global id did not advance")
        return False, ctx
    ctx["channel"] = ch
    ctx["ids"] = ids + [body["events"][0]["id"]]

    # Idempotent replay of identical content.
    status, _, body = _json(
        "POST", f"/events/{ch}", [_event("p1"), _event("p2")]
    )
    replayed_ids = [e["id"] for e in body["events"]]
    if status != 200 or replayed_ids != ids or body.get("replayed") != 2:
        print(f"[verify] publish FAILED: replay mismatch: {body}")
        return False, ctx

    # Different content -> 409 with locatable conflict and zero writes.
    status, _, body = _json(
        "POST",
        f"/events/{ch}",
        [
            _event("p1", message="MUTATED"),
            _event("p-never", severity="shutdown"),
        ],
    )
    if status != 409:
        print(f"[verify] publish FAILED: expected 409 got {status} {body}")
        return False, ctx
    conflict = (body or {}).get("conflicts", [{}])[0]
    if conflict.get("eventKey") != "p1" or "storedId" not in conflict:
        print(f"[verify] publish FAILED: 409 not locatable: {body}")
        return False, ctx

    # The never-seen key of the rejected batch must be absent.
    status, _, body = _json("POST", f"/events/{ch}",
                            [_event("p-never", severity="shutdown")])
    if status != 200 or body["events"][0].get("replayed"):
        print(f"[verify] publish FAILED: 409 batch leaked a write: {body}")
        return False, ctx

    print(
        "[verify] publish OK: global ids increasing, exact replay, "
        "409 zero-write with locatable conflict"
    )
    return True, ctx


def group_resume(ctx: dict) -> bool:
    print("\n[verify] == resume: Last-Event-ID continuation via SSE == ")
    sys.path.insert(0, str(TESTS_DIR))
    from helpers import SSEClient  # noqa: E402

    ch = ctx["channel"]
    ids = ctx["ids"]

    client = SSEClient(
        f"/streams/{ch}", headers={"Last-Event-ID": str(ids[0])}
    )
    client.start()
    try:
        got = [client.next_event(timeout=10)["id"] for _ in range(2)]
        if got != ids[1:]:
            print(f"[verify] resume FAILED: history {got} != {ids[1:]}")
            return False
        client.wait_for_banner(timeout=5)

        status, _, body = _json(
            "POST", f"/events/{ch}",
            [_event("p-live", severity="shutdown", message="resume live")],
        )
        live_id = body["events"][0]["id"]
        event = client.next_event(timeout=10)
        if event["id"] != live_id:
            print(f"[verify] resume FAILED: live id {event['id']} != {live_id}")
            return False
        if event["data"]["severity"] != "shutdown":
            print("[verify] resume FAILED: live payload mismatch")
            return False
    finally:
        client.close()

    print("[verify] resume OK: history then live, no gap and no duplicate")
    return True


def group_expired_cursor() -> bool:
    print("\n[verify] == expired cursor: 410 + earliestAvailableId == ")
    import uuid

    _, _, health = _json("GET", "/health")
    limit = int(health["retentionLimit"])
    ch = f"verify-exp-{uuid.uuid4().hex[:10]}"

    posted: list[int] = []
    batch = 50
    while len(posted) < limit + batch // 2:
        start = len(posted)
        events = [
            _event(f"e{i}", severity="warning" if i % 2 else "shutdown")
            for i in range(start, min(start + batch, limit + batch // 2))
        ]
        status, _, body = _json("POST", f"/events/{ch}", events)
        if status != 200:
            print(f"[verify] expired cursor FAILED: publish {status} {body}")
            return False
        posted.extend(e["id"] for e in body["events"])

    # A cursor older than the channel's oldest retained id must yield 410.
    status, _, body = _json(
        "GET", f"/streams/{ch}", headers={"Last-Event-ID": "1"}
    )
    if status != 410:
        print(f"[verify] expired cursor FAILED: expected 410 got {status}")
        return False
    earliest = (body or {}).get("earliestAvailableId")
    if not isinstance(earliest, int) or earliest not in posted:
        print(f"[verify] expired cursor FAILED: bad earliestAvailableId {body}")
        return False
    if earliest != posted[len(posted) - limit]:
        print(
            f"[verify] expired cursor FAILED: earliest {earliest} does not "
            f"match retention boundary"
        )
        return False

    # A cursor at earliest-1 is still expired...
    status, _, body = _json(
        "GET", f"/streams/{ch}",
        headers={"Last-Event-ID": str(earliest - 1)},
    )
    if status != 410 or body.get("earliestAvailableId") != earliest:
        print(f"[verify] expired cursor FAILED: boundary check {status} {body}")
        return False

    # ...while resuming exactly at earliest streams the retained window and
    # a fresh client (no cursor) starts at earliestAvailableId.
    sys.path.insert(0, str(TESTS_DIR))
    from helpers import SSEClient  # noqa: E402

    client = SSEClient(
        f"/streams/{ch}", headers={"Last-Event-ID": str(earliest - 1)}
    )  # expect non-200
    client.start()
    client.join(timeout=5)
    if client.status != 410:
        print(f"[verify] expired cursor FAILED: raw SSE status {client.status}")
        client.close()
        return False
    client.close()

    fresh = SSEClient(f"/streams/{ch}")
    fresh.start()
    try:
        first = fresh.next_event(timeout=10)
        if first["id"] != earliest:
            print(
                f"[verify] expired cursor FAILED: fresh client starts at "
                f"{first['id']} not earliest {earliest}"
            )
            return False
        expected = posted[posted.index(earliest):]
        received = [first["id"]]
        for _ in range(len(expected) - 1):
            received.append(fresh.next_event(timeout=10)["id"])
        if received != expected:
            print("[verify] expired cursor FAILED: retained window mismatch")
            return False
    finally:
        fresh.close()

    print(
        f"[verify] expired cursor OK: 410 earliestAvailableId={earliest}, "
        f"fresh stream replays {limit} retained events in order"
    )
    return True


def group_code_tests() -> bool:
    print("\n[verify] == code tests: unittest suite (unit + black box) == ")
    sys.path.insert(0, str(REPO_ROOT))
    sys.path.insert(0, str(TESTS_DIR))
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for module_name in ("test_storage", "test_ingest", "test_stream"):
        try:
            suite.addTests(loader.loadTestsFromName(module_name))
        except Exception as exc:
            print(f"[verify] could not load {module_name}: {exc!r}")
            return False
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


def main() -> int:
    failures = 0

    if not wait_for_healthy():
        print("[verify] aborting: service never became healthy")
        return (
            FAIL_CODE_TESTS
            | FAIL_BUILD_PUBLISH
            | FAIL_RESUME
            | FAIL_EXPIRED_CURSOR
        )

    # Group 1: build + publish (publish context feeds the resume group).
    build_ok = group_build()
    publish_ok, ctx = group_publish()
    if not build_ok or not publish_ok:
        failures |= FAIL_BUILD_PUBLISH

    # Group 2: resume (needs published ids).
    if publish_ok and not group_resume(ctx):
        failures |= FAIL_RESUME

    # Group 3: expired cursor smoke (self-contained).
    if not group_expired_cursor():
        failures |= FAIL_EXPIRED_CURSOR

    # Group 0: full code test suite last so smoke results print even if a
    # long streaming test fails.
    if not group_code_tests():
        failures |= FAIL_CODE_TESTS

    print("\n[verify] ================= summary ================")
    for bit, name in GROUP_NAMES.items():
        print(f"[verify]   {'PASS' if not failures & bit else 'FAIL'}  {name}")
    if failures == 0:
        print("[verify] ALL GROUPS PASSED")
    else:
        print(f"[verify] FAILED GROUPS BITMASK: {failures}")
    return failures


if __name__ == "__main__":
    raise SystemExit(main())
