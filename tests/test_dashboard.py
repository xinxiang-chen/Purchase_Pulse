from copy import deepcopy
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


def load_module(name, file):
    spec = importlib.util.spec_from_file_location(name, ROOT / file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


state = load_module("dashboard_state", "web/state.py")
generator = load_module("synthetic_data", "ingestion/generate_synthetic.py")
producer = load_module("csv_producer", "ingestion/producer.py")
CATALOG = json.loads((ROOT / "config/us_states.json").read_text())


class DashboardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "dashboard.sqlite3"
        self.store = state.DashboardStore(self.path, CATALOG)
        self.now = 1700000004
        self.batch = {
            "schema_version": 2, "batch_key": "test-batch-1",
            "type": "purchases",
            "replay": {"id": "test-round-1", "number": 1,
                       "started_at": datetime.fromtimestamp(self.now, timezone.utc).isoformat(),
                       "source_topic": "test-input", "start_offset": 0},
            "processed_at": datetime.fromtimestamp(self.now, timezone.utc).isoformat(),
            "totals": {"purchases": 2, "revenue_cents": 500},
            "age_groups": [{"label": label, "count": 2 if index == 0 else 0} for index, label in enumerate(state.AGE_GROUPS)],
            "states": [{"code": "CA", "count": 1, "revenue_cents": 201}, {"code": "NY", "count": 1, "revenue_cents": 299}],
        }

    def test_retry_and_restart_preserve_totals(self):
        self.assertTrue(self.store.apply_batch(self.batch))
        self.assertFalse(self.store.apply_batch(self.batch))
        restarted = state.DashboardStore(self.path, CATALOG)
        self.assertFalse(restarted.apply_batch(self.batch))
        snapshot = restarted.snapshot(self.now)
        self.assertEqual(snapshot["totals"], {"purchases": 2, "revenue": 5.0, "average_purchase": 2.5, "active_states": 2})
        self.assertEqual(len(snapshot["states"]), 50)
        self.assertEqual(sum(row["count"] for row in snapshot["age_groups"]), 2)

    def test_bad_batch_never_partially_changes_totals(self):
        broken = deepcopy(self.batch)
        broken["states"][0]["count"] = 3
        with self.assertRaises(ValueError):
            self.store.apply_batch(broken)
        self.assertEqual(self.store.snapshot(self.now)["totals"]["purchases"], 0)
        self.assertTrue(self.store.apply_batch(self.batch))

    def test_idle_trend_decays_but_current_round_totals_remain(self):
        self.store.apply_batch(self.batch)
        recent = self.store.snapshot(self.now)
        self.assertEqual(len(recent["trend"]), 60)
        self.assertEqual(sum(point["purchases"] for point in recent["trend"]), 2)
        idle = self.store.snapshot(self.now + 305)
        self.assertEqual(sum(point["purchases"] for point in idle["trend"]), 0)
        self.assertEqual(idle["totals"]["purchases"], 2)

    def test_malformed_timestamp_rejected_cleanly(self):
        self.batch["processed_at"] = 123
        with self.assertRaises(ValueError):
            self.store.apply_batch(self.batch)
        with self.assertRaises(ValueError):
            self.store.apply_batch([])


class DataTests(unittest.TestCase):
    def test_reproducible_profiles_and_state_coverage(self):
        states = [row["code"] for row in CATALOG]
        rows = list(generator.generate_rows(3000, 42, states))
        self.assertEqual(rows, list(generator.generate_rows(3000, 42, states)))
        self.assertEqual({row["state"] for row in rows}, set(states))
        self.assertEqual(len({row["event_id"] for row in rows}), 3000)
        profiles = {}
        for row in rows:
            profile = (row["age"], row["state"])
            self.assertEqual(profiles.setdefault(row["user_id"], profile), profile)
        self.assertEqual(set(rows[0]), {"event_id", "user_id", "age", "state", "amount", "event_time"})

    def test_decimal_currency_and_new_event_contract(self):
        row = {key: str(value) for key, value in next(generator.generate_rows(1, 42, [entry["code"] for entry in CATALOG])).items()}
        row["amount"] = "3.71"
        event = producer.parse_row(row, 2, {entry["code"] for entry in CATALOG})
        self.assertEqual(event["amount_cents"], 371)
        self.assertEqual(event["schema_version"], 2)
        self.assertNotIn("gender", event)
        self.assertNotIn("province", event)
        row["amount"] = "3.711"
        with self.assertRaises(ValueError):
            producer.parse_row(row, 2, {entry["code"] for entry in CATALOG})


if __name__ == "__main__":
    unittest.main()
