from copy import deepcopy
import importlib.util
from pathlib import Path
import sqlite3
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("round_dashboard_state", ROOT / "web/state.py")
state = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(state)
CATALOG = [
    {"code": "CA", "name": "California"},
    {"code": "NY", "name": "New York"},
    {"code": "TX", "name": "Texas"},
]


class RoundStateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "dashboard.sqlite3"
        self.store = state.DashboardStore(self.path, CATALOG)
        self.now = 1700000004

    def replay(self, number=1, offset=100):
        return {
            "id": f"replay-{number}", "number": number,
            "started_at": state.iso_timestamp(self.now + 10 * (number - 1)),
            "source_topic": "shopping-events", "start_offset": offset,
        }

    def batch(self, key="batch-1", replay=None, *, marker=False, timestamp=None):
        replay = deepcopy(replay if replay is not None else self.replay())
        return {
            "schema_version": 2, "type": "round_start" if marker else "purchases",
            "replay": replay, "batch_key": key,
            "processed_at": state.iso_timestamp(self.now if timestamp is None else timestamp),
            "totals": {"purchases": 0 if marker else 2, "revenue_cents": 0 if marker else 500},
            "age_groups": [
                {"label": label, "count": 2 if index == 0 and not marker else 0}
                for index, label in enumerate(state.AGE_GROUPS)
            ],
            "states": [] if marker else [
                {"code": "CA", "count": 1, "revenue_cents": 201},
                {"code": "NY", "count": 1, "revenue_cents": 299},
            ],
        }

    def assert_zero_statistics(self, snapshot):
        self.assertEqual(snapshot["totals"], {
            "purchases": 0, "revenue": 0, "average_purchase": 0, "active_states": 0,
        })
        self.assertTrue(all(row["count"] == 0 for row in snapshot["age_groups"]))
        self.assertTrue(all(row["count"] == 0 and row["revenue"] == 0 for row in snapshot["states"]))
        self.assertEqual(len(snapshot["trend"]), 60)
        self.assertTrue(all(row["purchases"] == 0 and row["revenue"] == 0 for row in snapshot["trend"]))

    def test_next_round_clears_kpis_age_states_and_trend(self):
        self.store.apply_batch(self.batch())
        second = self.replay(2, 200)
        marker = self.batch("round-2-start", second, marker=True, timestamp=self.now + 10)
        self.assertTrue(self.store.apply_batch(marker))
        snapshot = self.store.snapshot(self.now + 10)
        self.assert_zero_statistics(snapshot)
        self.assertEqual(snapshot["replay"], second)

        purchase = self.batch("round-2-purchases", second, timestamp=self.now + 11)
        purchase["totals"] = {"purchases": 1, "revenue_cents": 1200}
        purchase["age_groups"] = [
            {"label": label, "count": int(index == 1)}
            for index, label in enumerate(state.AGE_GROUPS)
        ]
        purchase["states"] = [{"code": "TX", "count": 1, "revenue_cents": 1200}]
        self.store.apply_batch(purchase)
        snapshot = self.store.snapshot(self.now + 11)
        self.assertEqual(snapshot["totals"], {
            "purchases": 1, "revenue": 12, "average_purchase": 12, "active_states": 1,
        })
        self.assertEqual([row["count"] for row in snapshot["age_groups"]], [0, 1, 0, 0, 0])
        self.assertEqual([row["count"] for row in snapshot["states"]], [0, 0, 1])
        self.assertEqual(sum(row["purchases"] for row in snapshot["trend"]), 1)
        self.assertEqual(sum(row["revenue"] for row in snapshot["trend"]), 12)

    def test_old_round_messages_and_markers_cannot_roll_back_new_round(self):
        self.store.apply_batch(self.batch())
        self.store.apply_batch(self.batch("new-round", self.replay(2, 200), timestamp=self.now + 10))
        before = self.store.snapshot(self.now + 10)
        self.assertFalse(self.store.apply_batch(self.batch("late-old-purchases")))
        self.assertFalse(self.store.apply_batch(self.batch("late-old-start", marker=True)))
        self.assertEqual(self.store.snapshot(self.now + 10), before)

    def test_duplicate_batches_count_once_within_current_round(self):
        purchase = self.batch()
        self.assertTrue(self.store.apply_batch(purchase))
        self.assertFalse(self.store.apply_batch(purchase))
        self.assertTrue(self.store.apply_batch(self.batch("batch-2")))
        self.assertEqual(self.store.snapshot(self.now)["totals"]["purchases"], 4)

    def test_restart_preserves_current_round_and_rejects_old_replays(self):
        self.store.apply_batch(self.batch())
        second = self.batch("round-2", self.replay(2, 200), timestamp=self.now + 10)
        self.store.apply_batch(second)
        before = self.store.snapshot(self.now + 10)
        restarted = state.DashboardStore(self.path, CATALOG)
        self.assertEqual(restarted.snapshot(self.now + 10), before)
        self.assertFalse(restarted.apply_batch(second))
        self.assertFalse(restarted.apply_batch(self.batch("late-after-restart")))
        self.assertEqual(restarted.snapshot(self.now + 10), before)

    def test_late_start_marker_does_not_clear_current_round_purchases(self):
        self.store.apply_batch(self.batch())
        late_start = self.batch("late-start", marker=True, timestamp=self.now + 1)
        self.assertTrue(self.store.apply_batch(late_start))
        self.assertFalse(self.store.apply_batch(late_start))
        snapshot = self.store.snapshot(self.now + 1)
        self.assertEqual(snapshot["totals"]["purchases"], 2)
        self.assertEqual(sum(row["purchases"] for row in snapshot["trend"]), 2)

    def test_first_purchase_can_advance_round_before_its_late_marker(self):
        self.store.apply_batch(self.batch())
        second = self.replay(2, 200)
        self.store.apply_batch(self.batch("round-2-first", second, timestamp=self.now + 10))
        self.store.apply_batch(self.batch("round-2-late-marker", second, marker=True, timestamp=self.now + 11))
        snapshot = self.store.snapshot(self.now + 11)
        self.assertEqual(snapshot["replay"], second)
        self.assertEqual(snapshot["totals"]["purchases"], 2)
        self.assertEqual(sum(row["purchases"] for row in snapshot["trend"]), 2)

    def test_invalid_new_round_payload_never_clears_current_statistics(self):
        self.store.apply_batch(self.batch())
        before = self.store.snapshot(self.now)
        bad_payloads = []
        inconsistent = self.batch("bad-count", self.replay(2, 200))
        inconsistent["states"][0]["count"] += 1
        bad_payloads.append(inconsistent)
        nonzero_marker = self.batch("bad-marker", self.replay(2, 200))
        nonzero_marker["type"] = "round_start"
        bad_payloads.append(nonzero_marker)
        for field, value in [
            ("id", ""), ("number", 0), ("number", True),
            ("start_offset", -1), ("start_offset", True),
            ("started_at", "2023-01-01T00:00:00"), ("source_topic", ""),
        ]:
            bad = self.batch("bad-replay", self.replay(2, 200))
            bad["replay"][field] = value
            bad_payloads.append(bad)
        legacy = self.batch("missing-replay")
        del legacy["replay"]
        bad_payloads.append(legacy)
        for bad in bad_payloads:
            with self.subTest(payload=bad):
                with self.assertRaises(ValueError):
                    self.store.apply_batch(bad)
                self.assertEqual(self.store.snapshot(self.now), before)

    def test_conflicting_round_identity_and_input_topic_are_rejected(self):
        self.store.apply_batch(self.batch())
        before = self.store.snapshot(self.now)
        conflict = self.batch("conflict")
        conflict["replay"]["id"] = "different-round-at-same-offset"
        different_source = self.batch("different-source", self.replay(2, 200))
        different_source["replay"]["source_topic"] = "other-purchases"
        for bad in (conflict, different_source):
            with self.assertRaises(ValueError):
                self.store.apply_batch(bad)
            self.assertEqual(self.store.snapshot(self.now), before)

    def test_failed_sql_write_rolls_back_round_switch_and_clearing(self):
        self.store.apply_batch(self.batch())
        before = self.store.snapshot(self.now)
        overflow = self.batch("overflow", self.replay(2, 200))
        overflow["totals"]["purchases"] = 2 ** 63
        overflow["age_groups"][0]["count"] = 2 ** 63
        overflow["states"] = [{"code": "TX", "count": 2 ** 63, "revenue_cents": 500}]
        with self.assertRaises(OverflowError):
            self.store.apply_batch(overflow)
        self.assertEqual(self.store.snapshot(self.now), before)

    def test_legacy_database_migration_discards_unscoped_lifetime_statistics(self):
        legacy_path = Path(self.temp.name) / "legacy.sqlite3"
        with sqlite3.connect(legacy_path) as db:
            db.executescript("""
                CREATE TABLE batches (batch_key TEXT PRIMARY KEY, processed_at REAL NOT NULL,
                    purchases INTEGER NOT NULL, revenue_cents INTEGER NOT NULL);
                CREATE TABLE ages (label TEXT PRIMARY KEY, count INTEGER NOT NULL);
                CREATE TABLE states (code TEXT PRIMARY KEY, count INTEGER NOT NULL,
                    revenue_cents INTEGER NOT NULL);
                INSERT INTO batches VALUES ('legacy', 1700000004, 99, 12345);
                INSERT INTO ages VALUES ('18–24', 99);
                INSERT INTO states VALUES ('CA', 99, 12345);
            """)
        migrated = state.DashboardStore(legacy_path, CATALOG)
        snapshot = migrated.snapshot(self.now)
        self.assertIsNone(snapshot["replay"])
        self.assertIsNone(snapshot["updated_at"])
        self.assert_zero_statistics(snapshot)
        self.assertTrue(migrated.apply_batch(self.batch()))
        restarted = state.DashboardStore(legacy_path, CATALOG)
        self.assertEqual(restarted.snapshot(self.now)["totals"]["purchases"], 2)


if __name__ == "__main__":
    unittest.main()
