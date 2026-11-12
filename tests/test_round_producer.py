"""Verify replay boundaries without connecting to a Kafka broker."""

from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
import importlib.util
import io
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("round_producer", ROOT / "ingestion/producer.py")
producer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(producer)


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, duration):
        self.sleeps.append(duration)
        self.now += duration


class FakeKafkaFuture:
    def __init__(self, client, offset):
        self.client = client
        self.offset = offset

    def get(self, timeout):
        self.client.acknowledgements.append(self.offset)
        if self.offset == self.client.fail_offset:
            raise RuntimeError("ack failed")
        return SimpleNamespace(offset=self.offset, partition=0)


class FakeKafkaProducer:
    def __init__(self):
        self.records = []
        self.acknowledgements = []
        self.fail_offset = None
        self.flushes = 0
        self.closed = False

    def send(self, topic, *, partition, key, value):
        # All previous sends must be acknowledged before sending this record.
        if len(self.acknowledgements) != len(self.records):
            raise AssertionError("send happened before the previous acknowledgement")
        offset = len(self.records)
        self.records.append({"topic": topic, "partition": partition, "key": key, "value": value})
        return FakeKafkaFuture(self, offset)

    def flush(self, timeout):
        self.flushes += 1

    def close(self, timeout):
        self.closed = True


class RoundProducerTests(unittest.TestCase):
    def setUp(self):
        self.events = [{
            "schema_version": 2,
            "event_id": f"source-{index}",
            "user_id": f"user-{index}",
            "age": 25 + index,
            "state": "CA",
            "amount_cents": 1200 + index,
            "event_time": "2026-01-01T00:00:00.000Z",
        } for index in range(2)]
        self.client = FakeKafkaProducer()
        self.clock = FakeClock()
        self.factory = self.enterContext(patch("kafka.KafkaProducer", return_value=self.client))
        self.enterContext(patch.object(producer.time, "monotonic", self.clock.monotonic))
        self.enterContext(patch.object(producer.time, "sleep", self.clock.sleep))
        self.enterContext(patch.object(producer, "uuid4", return_value=SimpleNamespace(hex="run-one")))
        stamps = iter(
            (datetime(2026, 11, 8, tzinfo=timezone.utc) + timedelta(milliseconds=index)).isoformat()
            for index in range(1000)
        )
        self.enterContext(patch.object(producer, "utc_iso", side_effect=lambda: next(stamps)))
        self.enterContext(redirect_stdout(io.StringIO()))

    def send(self, **kwargs):
        rate = kwargs.pop("rate", 0)
        profile = kwargs.pop("profile", "steady")
        return producer.send_events(self.events, "localhost:9092", "shopping-events", rate, profile, **kwargs)

    def values(self):
        return [record["value"] for record in self.client.records]

    def test_each_cycle_starts_with_acknowledged_marker_and_offset(self):
        self.assertEqual(self.send(limit=5), 5)
        values = self.values()
        self.assertEqual([value["event_type"] for value in values], [
            "replay_start", "purchase", "purchase",
            "replay_start", "purchase", "purchase",
            "replay_start", "purchase",
        ])
        active_marker = None
        start_offset = None
        purchases = []
        for offset, value in enumerate(values):
            self.assertEqual(self.client.records[offset]["partition"], 0)
            self.assertEqual(self.client.records[offset]["topic"], "shopping-events")
            if value["event_type"] == "replay_start":
                self.assertEqual(set(value), {
                    "schema_version", "event_type", "replay_id", "replay_number", "replay_started_at",
                })
                active_marker = value
                start_offset = offset
            else:
                for field in ["replay_id", "replay_number", "replay_started_at"]:
                    self.assertEqual(value[field], active_marker[field])
                self.assertEqual(value["replay_start_offset"], start_offset)
                original = self.events[len(purchases) % len(self.events)]
                for field in ["schema_version", "user_id", "age", "state", "amount_cents", "event_time"]:
                    self.assertEqual(value[field], original[field])
                purchases.append(value)
        self.assertEqual([value["replay_id"] for value in purchases], [
            "run-one:1", "run-one:1", "run-one:2", "run-one:2", "run-one:3",
        ])
        self.assertEqual(len({value["event_id"] for value in purchases}), 5)
        self.assertEqual(len({value["ingested_at"] for value in purchases}), 5)
        self.assertEqual(self.client.acknowledgements, list(range(8)))
        self.assertEqual(self.client.flushes, 1)
        self.assertTrue(self.client.closed)
        self.assertNotIn("event_type", self.events[0])

    def test_exact_cycle_limit_does_not_emit_an_extra_reset_marker(self):
        self.assertEqual(self.send(limit=4), 4)
        self.assertEqual(len(self.values()), 6)
        self.assertEqual(self.values()[-1]["event_type"], "purchase")
        self.assertEqual([v["replay_number"] for v in self.values() if v["event_type"] == "replay_start"], [1, 2])

    def test_once_keeps_the_final_round_visible(self):
        self.assertEqual(self.send(loop=False, limit=20), 2)
        self.assertEqual([v["event_type"] for v in self.values()], ["replay_start", "purchase", "purchase"])

    def test_short_limit_counts_purchases_only(self):
        self.assertEqual(self.send(limit=1), 1)
        self.assertEqual([v["event_type"] for v in self.values()], ["replay_start", "purchase"])

    def test_empty_or_zero_limit_connects_to_nothing(self):
        self.assertEqual(producer.send_events([], "unused", "unused", 0, "steady"), 0)
        self.assertEqual(self.send(limit=0), 0)
        self.factory.assert_not_called()

    def test_markers_do_not_count_as_rate_limited_purchases(self):
        self.assertEqual(self.send(rate=5, limit=5), 5)
        self.assertAlmostEqual(self.clock.now, .8)
        self.assertEqual(len(self.clock.sleeps), 5)
        for sleep in self.clock.sleeps[1:]:
            self.assertAlmostEqual(sleep, .2)

    def test_wave_clock_continues_across_reset_markers(self):
        self.assertEqual(self.send(rate=5, profile="wave", limit=60), 60)
        durations = {round(value, 4) for value in self.clock.sleeps[1:]}
        self.assertIn(.4, durations)
        self.assertIn(.2, durations)
        self.assertEqual(sum(v["event_type"] == "replay_start" for v in self.values()), 30)

    def test_failed_marker_ack_stops_before_any_purchase(self):
        self.client.fail_offset = 0
        with self.assertRaisesRegex(RuntimeError, "ack failed"):
            self.send(limit=5)
        self.assertEqual(len(self.values()), 1)
        self.assertEqual(self.values()[0]["event_type"], "replay_start")
        self.assertEqual(self.client.flushes, 1)
        self.assertTrue(self.client.closed)

    def test_failed_purchase_ack_stops_before_next_round(self):
        self.client.fail_offset = 2
        with self.assertRaisesRegex(RuntimeError, "ack failed"):
            self.send(limit=5)
        self.assertEqual([v["event_type"] for v in self.values()], ["replay_start", "purchase", "purchase"])
        self.assertEqual(self.client.flushes, 1)
        self.assertTrue(self.client.closed)


if __name__ == "__main__":
    unittest.main()
