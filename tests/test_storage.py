"""In-process tests for the storage contract (retention, restart, ids)."""

from __future__ import annotations

import os
import tempfile
import threading
import unittest

from app.storage import (
    BatchValidationError,
    ConflictError,
    Store,
)
from app.validation import validate_batch


def ev(key, **kw):
    return validate_batch(
        [
            {
                "eventKey": key,
                "time": "2026-10-05T12:00:00Z",
                "severity": "alarm",
                "message": f"message of {key}",
                **kw,
            }
        ]
    )[0]


class StoreTemp:
    def __init__(self, retention=5):
        fd, self.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        os.unlink(self.path)  # let sqlite create it
        self.retention = retention

    def make(self):
        return Store(self.path, self.retention)

    def cleanup(self):
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(self.path + suffix)
            except FileNotFoundError:
                pass


class IdAndOrderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = StoreTemp()

    def tearDown(self):
        self.tmp.cleanup()

    def test_ids_increase_and_match_publish_order(self):
        store = self.tmp.make()
        batch = validate_batch([ev("a"), ev("b"), ev("c")])
        results, new = store.ingest_batch("ch", batch)
        self.assertEqual(len(new), 3)
        ids = [r["id"] for r in results]
        self.assertEqual(ids, sorted(ids))
        self.assertEqual([e.created_seq for e in new], [0, 1, 2])
        store.close()

    def test_same_key_same_content_replays_id(self):
        store = self.tmp.make()
        results, _ = store.ingest_batch("ch", validate_batch([ev("a")]))
        original_id = results[0]["id"]
        results2, new2 = store.ingest_batch("ch", validate_batch([ev("a")]))
        self.assertEqual(results2[0]["id"], original_id)
        self.assertTrue(results2[0]["replayed"])
        self.assertEqual(new2, [])
        store.close()

    def test_different_content_conflicts_and_writes_nothing(self):
        store = self.tmp.make()
        store.ingest_batch("ch", validate_batch([ev("a", message="v1"), ev("b")]))
        with self.assertRaises(ConflictError) as ctx:
            store.ingest_batch(
                "ch",
                validate_batch([ev("a", message="v1"),
                                ev("b", message="CHANGED"),
                                ev("c")]),
            )
        self.assertEqual(ctx.exception.event_key, "b")

        stored = {e.event_key: e for e in store.list_events("ch")}
        self.assertNotIn("c", stored, "new key in conflicting batch must not exist")
        self.assertEqual(stored["b"].message, ev("b")["message"])
        store.close()

    def test_duplicate_key_inside_batch_is_a_request_error(self):
        store = self.tmp.make()
        with self.assertRaises(BatchValidationError):
            store.ingest_batch(
                "ch", validate_batch([ev("a", message="x"), ev("a", message="y")])
            )
        store.close()

    def test_concurrent_batches_never_interleave_or_duplicate(self):
        big = StoreTemp(retention=10_000)
        self.addCleanup(big.cleanup)
        store = big.make()
        errors = []

        def worker(tid):
            try:
                for i in range(30):
                    store.ingest_batch(
                        f"ch-{tid % 2}",
                        validate_batch([ev(f"t{tid}-{i}")]),
                    )
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(t,))
                   for t in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        for ch in ("ch-0", "ch-1"):
            events = store.list_events(ch)
            self.assertEqual(len(events), 120)
            self.assertEqual(
                [e.id for e in events], sorted(e.id for e in events)
            )
        store.close()


class RetentionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = StoreTemp(retention=3)

    def tearDown(self):
        self.tmp.cleanup()

    def test_keeps_only_newest_per_channel(self):
        store = self.tmp.make()
        all_ids = []
        for i in range(7):
            results, _ = store.ingest_batch(
                "ch", validate_batch([ev(f"k{i}")])
            )
            all_ids.append(results[0]["id"])
        kept = store.list_events("ch")
        self.assertEqual(len(kept), 3)
        self.assertEqual([e.event_key for e in kept], ["k4", "k5", "k6"])

        earliest = store.earliest_available_id("ch")
        self.assertEqual(earliest, all_ids[4])
        store.close()

    def test_retention_is_per_channel(self):
        store = self.tmp.make()
        for i in range(5):
            store.ingest_batch("ch1", validate_batch([ev(f"a{i}")]))
        for i in range(2):
            store.ingest_batch("ch2", validate_batch([ev(f"b{i}")]))
        self.assertEqual(len(store.list_events("ch1")), 3)
        self.assertEqual(len(store.list_events("ch2")), 2)
        store.close()

    def test_history_after_an_evicted_id_reports_old_earliest(self):
        store = self.tmp.make()
        ids = []
        for i in range(6):
            r, _ = store.ingest_batch("ch", validate_batch([ev(f"k{i}")]))
            ids.append(r[0]["id"])
        events, earliest = store.history_after("ch", ids[1])
        self.assertEqual(earliest, ids[3])
        # Only retained events are returned (ids[3..5]); caller detects the
        # gap through earliest and the HTTP layer answers 410.
        self.assertEqual([e.id for e in events], ids[3:])
        store.close()

    def test_replays_survive_retention_trimming_of_other_events(self):
        store = self.tmp.make()
        target_id = None
        for i in range(5):
            r, _ = store.ingest_batch("ch", validate_batch([ev(f"k{i}")]))
            if i == 4:
                target_id = r[0]["id"]
        # k4 is within the newest 3; replay still works.
        r, new = store.ingest_batch("ch", validate_batch([ev("k4")]))
        self.assertEqual(r[0]["id"], target_id)
        self.assertTrue(r[0]["replayed"])
        self.assertEqual(new, [])
        store.close()


class RestartTests(unittest.TestCase):
    def setUp(self):
        self.tmp = StoreTemp(retention=100)

    def tearDown(self):
        self.tmp.cleanup()

    def test_ids_replays_and_conflicts_survive_restart(self):
        store = self.tmp.make()
        r1, _ = store.ingest_batch(
            "ch", validate_batch([ev("keep", message="v1"), ev("other")])
        )
        first_ids = [x["id"] for x in r1]
        last_id = first_ids[-1]
        store.close()

        # Simulate service restart: new connection to the same DB file.
        store = self.tmp.make()

        # New ids continue strictly above pre-restart ids.
        r2, _ = store.ingest_batch("ch", validate_batch([ev("after")]))
        self.assertGreater(r2[0]["id"], last_id)

        # Same content replays the exact original result.
        r3, _ = store.ingest_batch(
            "ch", validate_batch([ev("keep", message="v1")])
        )
        self.assertEqual(r3[0]["id"], first_ids[0])
        self.assertTrue(r3[0]["replayed"])

        # Different content still conflicts after restart, zero writes.
        before = {e.event_key: e for e in store.list_events("ch")}
        with self.assertRaises(ConflictError):
            store.ingest_batch(
                "ch", validate_batch([ev("keep", message="TAMPERED")])
            )
        after = {e.event_key: e for e in store.list_events("ch")}
        self.assertEqual(set(before), set(after))
        self.assertEqual(after["keep"].message, "v1")
        store.close()

    def test_retention_boundary_survives_restart(self):
        tmp = StoreTemp(retention=3)
        store = tmp.make()
        ids = []
        for i in range(6):
            r, _ = store.ingest_batch("ch", validate_batch([ev(f"k{i}")]))
            ids.append(r[0]["id"])
        store.close()

        store = tmp.make()
        self.assertEqual([e.id for e in store.list_events("ch")], ids[3:])
        self.assertEqual(store.earliest_available_id("ch"), ids[3])

        # New post-restart events keep the global sequence and trim again.
        r, _ = store.ingest_batch("ch", validate_batch([ev("k6")]))
        self.assertGreater(r[0]["id"], ids[-1])
        self.assertEqual(
            [e.event_key for e in store.list_events("ch")],
            ["k4", "k5", "k6"],
        )
        store.close()
        tmp.cleanup()


if __name__ == "__main__":
    unittest.main(verbosity=2)
