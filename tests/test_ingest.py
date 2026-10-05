"""Black-box tests run over HTTP against a live service.

The target URL comes from EVENT_SERVICE_URL (default local 8080). Channels
are prefixed uniquely so the suite is repeatable against a persisted volume.
"""

from __future__ import annotations

import json
import threading
import time
import unittest
import uuid

from helpers import (
    SSEClient,
    http_json,
    http_request,
    make_event,
    post_batch,
)


def unique_channel(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


class HealthTests(unittest.TestCase):
    def test_health_ok(self):
        status, _, body = http_json("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ok")

    def test_unknown_route(self):
        status, _, body = http_json("GET", "/nope")
        self.assertEqual(status, 404)
        self.assertEqual(body["error"], "not_found")


class IngestTests(unittest.TestCase):
    def test_new_keys_get_increasing_global_ids(self):
        ch = unique_channel("ing-new")
        status, _, body = post_batch(
            ch, [make_event("a"), make_event("b", severity="warning")]
        )
        self.assertEqual(status, 200, body)
        self.assertEqual(body["newEvents"], 2)
        self.assertEqual(body["replayed"], 0)
        ids = [e["id"] for e in body["events"]]
        self.assertEqual(len(set(ids)), 2)
        self.assertEqual(ids, sorted(ids), "ids assigned in batch order")
        self.assertEqual(
            [e["replayed"] for e in body["events"]], [False, False]
        )

        # Next batch keeps increasing in the global space.
        status, _, body2 = post_batch(ch, [make_event("c", severity="shutdown")])
        self.assertEqual(status, 200, body2)
        self.assertGreater(body2["events"][0]["id"], ids[-1])

    def test_batch_size_limits(self):
        ch = unique_channel("ing-size")
        status, _, body = post_batch(ch, [])
        self.assertEqual(status, 400)
        self.assertEqual(body["error"], "validation_failed")

        status, _, body = post_batch(
            ch, [make_event(f"k{i}") for i in range(51)]
        )
        self.assertEqual(status, 400)
        self.assertIn("1 and 50", body["message"])

        # Exactly 50 is accepted.
        status, _, body = post_batch(
            ch, [make_event(f"ok{i}") for i in range(50)]
        )
        self.assertEqual(status, 200, body)

    def test_required_fields_and_rfc3339(self):
        ch = unique_channel("ing-val")
        bad_bodies = [
            [{"eventKey": "x", "time": "2026-10-05T12:00:00Z",
              "severity": "alarm"}],  # missing message
            [{"eventKey": "x", "time": "not-a-time",
              "severity": "alarm", "message": "m"}],
            [{"eventKey": "x", "time": "2026-10-05T12:00:00",
              "severity": "alarm", "message": "m (no tz)"}],
            [{"eventKey": "x", "time": "2026-10-05T12:00:00Z",
              "severity": "explode", "message": "m"}],
            "not-an-array",
        ]
        for bad in bad_bodies:
            status, _, body = post_batch(ch, bad)
            self.assertEqual(status, 400, f"expected 400 for {bad!r}: {body}")
            self.assertEqual(body["error"], "validation_failed")

    def test_rfc3339_offset_accepted(self):
        ch = unique_channel("ing-tz")
        status, _, body = post_batch(
            ch,
            [
                make_event("z", time="2026-10-05T12:00:00Z"),
                make_event("off", time="2026-10-05T14:00:00+02:00"),
                make_event("frac", time="2026-10-05T12:00:00.123Z"),
            ],
        )
        self.assertEqual(status, 200, body)

    def test_bad_json_and_channel(self):
        import http.client
        import os
        from urllib.parse import urlsplit

        parts = urlsplit(
            os.environ.get("EVENT_SERVICE_URL", "http://127.0.0.1:8080")
        )
        conn = http.client.HTTPConnection(parts.hostname, parts.port or 80,
                                          timeout=10)
        conn.request(
            "POST",
            "/events/abc",
            body=b"{not json",
            headers={"Content-Type": "application/json",
                     "Content-Length": "9"},
        )
        resp = conn.getresponse()
        payload = json.loads(resp.read())
        self.assertEqual(resp.status, 400)
        self.assertEqual(payload["error"], "invalid_json")
        conn.close()

        # '%' is not in the allowed channel charset.
        status, _, body = http_json("GET", "/streams/US%20EAST")
        self.assertEqual(status, 400)
        self.assertEqual(body["error"], "invalid_channel")

    def test_duplicate_key_within_batch_rejected(self):
        ch = unique_channel("ing-dup")
        status, _, body = post_batch(
            ch, [make_event("x", message="one"), make_event("x", message="two")]
        )
        self.assertEqual(status, 400, body)

        # Nothing written: a clean post with key x must be "new".
        status, _, body = post_batch(ch, [make_event("x")])
        self.assertEqual(status, 200)
        self.assertFalse(body["events"][0]["replayed"])


class IdempotencyConflictTests(unittest.TestCase):
    def test_same_key_same_content_replays_original_result(self):
        ch = unique_channel("idem-ok")
        events = [make_event("r1", message="same"), make_event("r2")]
        s1, _, b1 = post_batch(ch, events)
        self.assertEqual(s1, 200, b1)
        first = b1["events"]

        # Same keys, same payload, different order even -> replay.
        s2, _, b2 = post_batch(ch, list(reversed(events)))
        self.assertEqual(s2, 200, b2)
        by_key = {e["eventKey"]: e for e in b2["events"]}
        self.assertTrue(all(e["replayed"] for e in b2["events"]))
        self.assertEqual(by_key["r1"]["id"], first[0]["id"])
        self.assertEqual(by_key["r2"]["id"], first[1]["id"])

        # Repeated replays return identical ids forever.
        s3, _, b3 = post_batch(ch, events)
        self.assertEqual(
            [e["id"] for e in b3["events"]], [e["id"] for e in first]
        )

    def test_replay_mixed_with_new_keys(self):
        ch = unique_channel("idem-mix")
        _, _, b1 = post_batch(ch, [make_event("m1")])
        existing_id = b1["events"][0]["id"]
        _, _, b2 = post_batch(ch, [make_event("m1"), make_event("m2")])
        self.assertEqual(b2["newEvents"], 1)
        self.assertEqual(b2["replayed"], 1)
        ids = {e["eventKey"]: e for e in b2["events"]}
        self.assertEqual(ids["m1"]["id"], existing_id)
        self.assertTrue(ids["m1"]["replayed"])
        self.assertFalse(ids["m2"]["replayed"])

    def test_conflicting_content_returns_409_with_zero_writes(self):
        ch = unique_channel("idem-409")
        s, _, b = post_batch(
            ch, [make_event("c1", message="original", severity="warning")]
        )
        self.assertEqual(s, 200, b)

        # Same key, different message; plus a brand new key that must NOT
        # be written.
        s, _, b = post_batch(
            ch,
            [
                make_event("c1", message="CHANGED", severity="warning"),
                make_event("c2", severity="shutdown", message="should not land"),
            ],
        )
        self.assertEqual(s, 409, b)
        self.assertEqual(b["error"], "conflict")
        self.assertEqual(b["conflicts"][0]["eventKey"], "c1")
        conflict_id = b["conflicts"][0]["storedId"]
        self.assertIsInstance(conflict_id, int)
        self.assertEqual(
            b["conflicts"][0]["stored"]["message"], "original"
        )
        self.assertEqual(
            b["conflicts"][0]["incoming"]["message"], "CHANGED"
        )

        # The new key from the rejected batch is genuinely absent: posting
        # identical content for it is a new insert, not a replay.
        s, _, b = post_batch(ch, [make_event("c2", severity="shutdown",
                                             message="should not land")])
        self.assertEqual(s, 200, b)
        self.assertFalse(b["events"][0]["replayed"])

        # Original event is untouched and replays with its original id.
        s, _, b = post_batch(
            ch, [make_event("c1", message="original", severity="warning")]
        )
        self.assertEqual(s, 200, b)
        self.assertTrue(b["events"][0]["replayed"])

    def test_conflict_on_each_content_field_is_locatable(self):
        ch = unique_channel("idem-fields")
        _, _, b0 = post_batch(
            ch, [make_event("f", time="2026-10-05T12:00:00Z",
                            severity="alarm", message="base")]
        )
        for field, changed in [
            ("time", {"time": "2026-10-05T12:00:01Z"}),
            ("severity", {"severity": "critical"}),
            ("message", {"message": "other"}),
        ]:
            event = make_event(
                "f", time="2026-10-05T12:00:00Z",
                severity="alarm", message="base",
            )
            event.update(changed)
            s, _, body = post_batch(ch, [event])
            self.assertEqual(s, 409, f"{field} change must conflict")
            self.assertEqual(body["conflicts"][0]["eventKey"], "f")

    def test_channels_are_independent(self):
        ch1 = unique_channel("iso-1")
        ch2 = unique_channel("iso-2")
        s, _, b1 = post_batch(ch1, [make_event("same-key", message="a")])
        self.assertEqual(s, 200)
        s, _, b2 = post_batch(ch2, [make_event("same-key", message="b")])
        self.assertEqual(s, 200)
        # Same key, different content on a different channel is fine.
        self.assertFalse(b2["events"][0]["replayed"])
        self.assertNotEqual(b1["events"][0]["id"], b2["events"][0]["id"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
