"""SSE behavior: replay/live seam, resume, exactly-once, heartbeats."""

from __future__ import annotations

import json
import threading
import time
import unittest
import uuid

from helpers import SSEClient, make_event, post_batch


def unique_channel(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def post_one(ch, key, **kw):
    status, _, body = post_batch(ch, [make_event(key, **kw)])
    assert status == 200, body
    return body["events"][0]["id"]


class LiveStreamTests(unittest.TestCase):
    def test_live_events_carry_global_sse_ids(self):
        ch = unique_channel("live-id")
        client = SSEClient(f"/streams/{ch}")
        client.start()
        client.wait_for_banner()

        new_id = post_one(ch, "e1", severity="shutdown")
        event = client.next_event(timeout=5)
        self.assertEqual(event["id"], new_id)
        self.assertEqual(event["data"]["eventKey"], "e1")
        self.assertEqual(event["data"]["severity"], "shutdown")
        client.close()

    def test_channel_isolation(self):
        ch1 = unique_channel("live-iso-a")
        ch2 = unique_channel("live-iso-b")
        client = SSEClient(f"/streams/{ch1}")
        client.start()
        client.wait_for_banner()

        post_one(ch2, "other")
        post_one(ch1, "mine")
        event = client.next_event(timeout=5)
        self.assertEqual(event["data"]["eventKey"], "mine")

        # Nothing else must arrive promptly from the other channel.
        with self.assertRaises(Exception):
            client.next_event(timeout=1.0)
        client.close()

    def test_idle_heartbeat_within_five_seconds(self):
        ch = unique_channel("live-hb")
        client = SSEClient(f"/streams/{ch}")
        client.start()
        client.wait_for_banner()

        deadline = time.monotonic() + 7.0
        saw_heartbeat = False
        while time.monotonic() < deadline:
            item = client.next_item(timeout=max(0.1, deadline - time.monotonic()))
            if item["type"] == "comment" and item["value"].startswith("heartbeat"):
                saw_heartbeat = True
                break
        self.assertTrue(saw_heartbeat, "no heartbeat comment within 5s idle")
        client.close()

    def test_unknown_channel_opens_then_receives_live(self):
        ch = unique_channel("live-fresh")
        client = SSEClient(f"/streams/{ch}")
        client.start()
        client.wait_for_banner()
        new_id = post_one(ch, "first")
        event = client.next_event(timeout=5)
        self.assertEqual(event["id"], new_id)
        client.close()


class ResumeTests(unittest.TestCase):
    def test_last_event_id_replays_history_then_live_once(self):
        ch = unique_channel("res-seam")
        id1 = post_one(ch, "a")
        id2 = post_one(ch, "b")
        id3 = post_one(ch, "c")

        client = SSEClient(
            f"/streams/{ch}", headers={"Last-Event-ID": str(id1)}
        )
        client.start()

        # History portion first, strictly after the cursor.
        first = client.next_event(timeout=5)
        second = client.next_event(timeout=5)
        self.assertEqual([first["id"], second["id"]], [id2, id3])

        # Seamless transition into live events, each exactly once.
        client.wait_for_banner()
        id4 = post_one(ch, "d")
        live = client.next_event(timeout=5)
        self.assertEqual(live["id"], id4)

        seen = [first["id"], second["id"], live["id"]]
        self.assertEqual(len(seen), len(set(seen)))
        client.close()

    def test_resume_via_query_parameter(self):
        ch = unique_channel("res-query")
        id1 = post_one(ch, "a")
        id2 = post_one(ch, "b")
        client = SSEClient(f"/streams/{ch}?lastEventId={id1}")
        client.start()
        event = client.next_event(timeout=5)
        self.assertEqual(event["id"], id2)
        client.close()

    def test_reconnect_after_outage_misses_nothing_and_shows_no_dupes(self):
        """Console scenario: disconnect, events keep arriving, reconnect."""
        ch = unique_channel("res-gap")
        a = post_one(ch, "stop-1", severity="shutdown", message="trip A")
        b = post_one(ch, "alarm-1", severity="alarm", message="alarm B")

        first = SSEClient(f"/streams/{ch}",
                          headers={"Last-Event-ID": str(a)})
        first.start()
        got = first.next_event(timeout=5)
        self.assertEqual(got["id"], b)
        first.close()
        time.sleep(0.3)  # simulate network blip / reconnect delay

        # Events published while the console is disconnected.
        c = post_one(ch, "stop-2", severity="shutdown", message="trip C")
        d = post_one(ch, "alarm-2", severity="critical", message="alarm D")

        second = SSEClient(f"/streams/{ch}",
                           headers={"Last-Event-ID": str(b)})
        second.start()
        recovered = [second.next_event(timeout=5)["id"] for _ in range(2)]
        self.assertEqual(recovered, [c, d])

        # The already-displayed events are never re-shown.
        second.wait_for_banner()
        e = post_one(ch, "alarm-3", severity="warning", message="alarm E")
        only = second.next_event(timeout=5)
        self.assertEqual(only["id"], e)
        client_ids = recovered + [only["id"]]
        self.assertNotIn(a, client_ids)
        self.assertNotIn(b, client_ids)
        second.close()

    def test_content_type_is_event_stream(self):
        ch = unique_channel("res-ct")
        client = SSEClient(f"/streams/{ch}")
        client.start()
        client.wait_for_banner()
        self.assertTrue(
            client.resp_headers["content-type"].startswith("text/event-stream")
        )
        client.close()

    def test_invalid_cursor_rejected(self):
        ch = unique_channel("res-badcursor")
        client = SSEClient(
            f"/streams/{ch}", headers={"Last-Event-ID": "abc"}
        )
        client.start()
        client.join(timeout=5)
        self.assertEqual(client.status, 400)
        body = json.loads(client.error_body)
        self.assertEqual(body["error"], "invalid_cursor")


class ConcurrentPublishTests(unittest.TestCase):
    def test_concurrent_publishers_each_event_once_and_in_id_order(self):
        ch = unique_channel("conc-once")
        client = SSEClient(f"/streams/{ch}")
        client.start()
        client.wait_for_banner()

        n_threads = 4
        per_thread = 25

        def worker(tid: int):
            keys = [f"t{tid}-k{i}" for i in range(per_thread)]
            # Send in small concurrent batches to stress interleaving.
            for start in range(0, per_thread, 5):
                status, _, body = post_batch(
                    ch,
                    [make_event(k, severity="alarm",
                                message=f"m {k}")
                     for k in keys[start:start + 5]],
                )
                self.assertEqual(status, 200, body)

        threads = [threading.Thread(target=worker, args=(t,))
                   for t in range(n_threads)]
        for t in threads:
            t.start()

        total = n_threads * per_thread
        received: list[int] = []
        deadline = time.monotonic() + 30
        while len(received) < total and time.monotonic() < deadline:
            event = client.next_event(timeout=deadline - time.monotonic())
            received.append(event["id"])

        for t in threads:
            t.join(timeout=10)

        self.assertEqual(len(received), total, "lost or duplicated an event")
        self.assertEqual(len(received), len(set(received)),
                         "an event was delivered more than once")
        self.assertEqual(received, sorted(received),
                         "live events must follow global id order")
        client.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
