from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
import importlib.util
import io
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]


def load_module(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


producer = load_module("looping_producer", "ingestion/producer.py")
generator = load_module("atomic_generator", "ingestion/generate_synthetic.py")


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, duration):
        self.sleeps.append(duration)
        self.now += duration


class ProducerTests(unittest.TestCase):
    def setUp(self):
        self.events = [{
            "schema_version": 2, "event_id": f"source-{index}", "user_id": f"user-{index}",
            "age": 24 + index, "state": "CA", "amount_cents": 371,
            "event_time": "2026-01-01T00:00:00.000Z",
        } for index in range(2)]
        self.output = io.StringIO()
        self.client = Mock()
        self.client.send.return_value.get.return_value = SimpleNamespace(offset=0)
        self.clock = FakeClock()
        self.factory = self.enterContext(patch("kafka.KafkaProducer", return_value=self.client))
        self.enterContext(patch.object(producer.time, "monotonic", self.clock.monotonic))
        self.enterContext(patch.object(producer.time, "sleep", self.clock.sleep))
        self.enterContext(patch.object(producer, "uuid4", return_value=SimpleNamespace(hex="run-one")))
        stamps = iter((datetime(2026, 11, 8, tzinfo=timezone.utc) + timedelta(seconds=i)).isoformat() for i in range(1000))
        self.enterContext(patch.object(producer, "utc_iso", side_effect=lambda: next(stamps)))
        self.enterContext(redirect_stdout(self.output))

    def send(self, **kwargs):
        return producer.send_events(self.events, "localhost:9092", "test-topic", 0, "steady", **kwargs)

    def payloads(self):
        return [call.kwargs["value"] for call in self.client.send.call_args_list
                if call.kwargs["value"].get("event_type") == "purchase"]

    def test_limit_crosses_eof_and_new_cycles_have_fresh_ids(self):
        self.assertEqual(self.send(limit=5), 5)
        payloads = self.payloads()
        self.assertEqual([value["event_id"] for value in payloads], [
            "source-0:run-one:1", "source-1:run-one:1", "source-0:run-one:2",
            "source-1:run-one:2", "source-0:run-one:3",
        ])
        self.assertEqual(len({value["ingested_at"] for value in payloads}), 5)
        for index, value in enumerate(payloads):
            original = self.events[index % 2]
            for field in ["user_id", "age", "state", "amount_cents", "event_time"]:
                self.assertEqual(value[field], original[field])
        self.client.flush.assert_called_once_with(timeout=30)
        self.client.close.assert_called_once_with(timeout=30)

    def test_once_stops_at_eof_even_with_a_larger_limit(self):
        self.assertEqual(self.send(loop=False, limit=7), 2)
        self.assertEqual(len(self.payloads()), 2)

    def test_steady_rate_keeps_pacing_across_cycles(self):
        sent = producer.send_events(self.events, "localhost:9092", "test", 5, "steady", limit=5)
        self.assertEqual(sent, 5)
        self.assertAlmostEqual(self.clock.now, .8)
        self.assertEqual(len(self.clock.sleeps), 5)
        for duration in self.clock.sleeps[1:]:
            self.assertAlmostEqual(duration, .2)

    def test_wave_timer_does_not_restart_at_each_eof(self):
        producer.send_events(self.events, "localhost:9092", "test", 5, "wave", limit=60)
        durations = {round(value, 4) for value in self.clock.sleeps[1:]}
        self.assertIn(.4, durations)
        self.assertIn(.2, durations)

    def test_no_connection_for_empty_data_or_zero_limit(self):
        self.assertEqual(producer.send_events([], "localhost:9092", "test", 0, "steady"), 0)
        self.assertEqual(self.send(limit=0), 0)
        self.factory.assert_not_called()

    def test_failed_ack_stops_replay_and_closes_client(self):
        self.client.send.return_value.get.side_effect = RuntimeError("send failed")
        with self.assertRaisesRegex(RuntimeError, "send failed"):
            self.send(limit=5)
        self.client.flush.assert_called_once_with(timeout=30)
        self.client.close.assert_called_once_with(timeout=30)

    def test_cli_limit_applies_to_all_cycles_and_defaults_to_five_per_second(self):
        with patch.object(producer, "load_state_codes", return_value={"CA"}), \
             patch.object(producer, "read_events", return_value=self.events) as reader, \
             patch.object(producer, "send_events", return_value=5) as sender:
            self.assertEqual(producer.main(["--limit", "5"]), 0)
        self.assertIsNone(reader.call_args.kwargs["limit"])
        self.assertEqual(sender.call_args.args[3], 5)
        self.assertEqual(sender.call_args.kwargs, {"loop": True, "limit": 5})


class AtomicGenerationTests(unittest.TestCase):
    def test_failed_generation_leaves_the_previous_dataset_intact(self):
        def broken_rows(*args):
            yield {field: "test" for field in generator.FIELDS}
            raise RuntimeError("generation failed")

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "user_log.csv"
            output.write_text("previous complete dataset\n")
            with patch.object(generator, "load_states", return_value=["CA"]), \
                 patch.object(generator, "generate_rows", side_effect=broken_rows):
                with self.assertRaisesRegex(RuntimeError, "generation failed"):
                    generator.main(["--rows", "2", "--output", str(output), "--force"])
            self.assertEqual(output.read_text(), "previous complete dataset\n")
            self.assertEqual(sorted(path.name for path in Path(directory).iterdir()), ["user_log.csv"])


if __name__ == "__main__":
    unittest.main()
