"""Continuously replay the US demo CSV as fresh version-2 purchase events."""

import argparse
import csv
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
import math
from pathlib import Path
import sys
import time
from uuid import uuid4


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REQUIRED_FIELDS = {"event_id", "user_id", "age", "state", "amount", "event_time"}
WAVE_MULTIPLIERS = (0.5, 1.0, 1.5, 1.0)


def utc_iso(value=None):
    return (value or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")


def load_state_codes():
    entries = json.loads((PROJECT_ROOT / "config/us_states.json").read_text(encoding="utf-8"))
    codes = {entry["code"] for entry in entries}
    if len(entries) != 50 or len(codes) != 50 or "DC" in codes:
        raise ValueError("Expected a catalog containing the 50 US states")
    return codes


def parse_row(row, line_number, states):
    def fail(message):
        raise ValueError(f"CSV line {line_number}: {message}")

    if None in row:
        fail("more values than header columns")
    values = {field: (row.get(field) or "").strip() for field in REQUIRED_FIELDS}
    for field, value in values.items():
        if not value:
            fail(f"{field} is required")
    try:
        age = int(values["age"])
    except ValueError:
        fail("age must be a whole number between 18 and 80")
    if not 18 <= age <= 80:
        fail("age must be between 18 and 80")
    state = values["state"].upper()
    if state not in states:
        fail("state must be a two-letter code from config/us_states.json")
    try:
        amount = Decimal(values["amount"])
        if not amount.is_finite() or amount <= 0 or amount > Decimal("92233720368547758.07"):
            fail("amount must be positive USD within the 64-bit cents range")
        cents = amount * 100
        if cents != cents.to_integral_value():
            fail("amount must have no more than two fractional USD digits")
        amount_cents = int(cents)
    except InvalidOperation:
        fail("amount must be a valid decimal USD amount")
    try:
        event_time = datetime.fromisoformat(values["event_time"].replace("Z", "+00:00"))
        if event_time.tzinfo is None:
            fail("event_time must include a UTC offset or Z")
    except ValueError:
        fail("event_time must be an ISO timestamp including a UTC offset or Z")
    return {
        "schema_version": 2,
        "event_id": values["event_id"],
        "user_id": values["user_id"],
        "age": age,
        "state": state,
        "amount_cents": amount_cents,
        "event_time": utc_iso(event_time),
    }


def read_events(path, states, limit=None):
    """Validate the selected rows before opening a connection to Kafka."""
    events = []
    event_ids = set()
    profiles = {}
    with path.open(encoding="utf-8-sig", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        fields = reader.fieldnames or []
        if {"gender", "province"}.intersection(fields):
            raise ValueError(
                "Legacy CSV format detected. Generate the US dataset with "
                "ingestion/generate_synthetic.py --force; expected age/state/amount fields."
            )
        if len(fields) != len(set(fields)):
            raise ValueError("CSV contains duplicate header columns")
        missing = REQUIRED_FIELDS.difference(fields)
        if missing:
            raise ValueError("CSV is missing required fields: " + ", ".join(sorted(missing)))
        for row in reader:
            if limit is not None and len(events) >= limit:
                break
            event = parse_row(row, reader.line_num, states)
            if event["event_id"] in event_ids:
                raise ValueError(f"CSV line {reader.line_num}: duplicate event_id {event['event_id']}")
            profile = (event["age"], event["state"])
            previous = profiles.setdefault(event["user_id"], profile)
            if previous != profile:
                raise ValueError(
                    f"CSV line {reader.line_num}: age/state changed for user {event['user_id']}"
                )
            event_ids.add(event["event_id"])
            events.append(event)
    return events


def send_events(events, bootstrap_servers, topic, rate, profile, *, loop=True, limit=None):
    """Keep ordered replay markers and purchases on the single input partition."""
    if not events or limit == 0:
        return 0
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=lambda value: json.dumps(
            value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        ).encode("utf-8"),
        acks="all",
        retries=3,
    )
    sent = 0
    cycle = 1
    run_id = uuid4().hex
    start = time.monotonic()
    next_send = start
    try:
        while True:
            replay_id = f"{run_id}:{cycle}"
            replay_started_at = utc_iso()
            marker = {
                "schema_version": 2,
                "event_type": "replay_start",
                "replay_id": replay_id,
                "replay_number": cycle,
                "replay_started_at": replay_started_at,
            }
            metadata = producer.send(
                topic, partition=0, key=b"replay-start", value=marker
            ).get(timeout=30)
            replay_start_offset = metadata.offset
            if not isinstance(replay_start_offset, int) or replay_start_offset < 0:
                raise ValueError("Replay-start acknowledgement did not include a valid Kafka offset")
            for event in events:
                if rate > 0:
                    time.sleep(max(0, next_send - time.monotonic()))
                    multiplier = (
                        WAVE_MULTIPLIERS[int((time.monotonic() - start) // 10) % len(WAVE_MULTIPLIERS)]
                        if profile == "wave" else 1.0
                    )
                    next_send = time.monotonic() + 1 / (rate * multiplier)
                payload = {
                    **event,
                    "event_type": "purchase",
                    "event_id": f"{event['event_id']}:{replay_id}",
                    "replay_id": replay_id,
                    "replay_number": cycle,
                    "replay_started_at": replay_started_at,
                    "replay_start_offset": replay_start_offset,
                    "ingested_at": utc_iso(),
                }
                producer.send(
                    topic, partition=0, key=event["user_id"].encode("utf-8"), value=payload
                ).get(timeout=30)
                sent += 1
                if limit is not None and sent >= limit:
                    return sent
            print(f"Completed cycle {cycle}: {len(events):,} events ({sent:,} sent in total).", flush=True)
            if not loop:
                return sent
            cycle += 1
            print(f"Starting cycle {cycle} from the first CSV row.", flush=True)
    finally:
        try:
            producer.flush(timeout=30)
        finally:
            producer.close(timeout=30)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=PROJECT_ROOT / "data/user_log.csv")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="shopping-events")
    parser.add_argument("--rate", type=float, default=5.0, help="Base events/second; 0 sends without pacing.")
    parser.add_argument("--profile", choices=("wave", "steady"), default="wave")
    parser.add_argument("--limit", type=int, help="Maximum successful purchase sends across all cycles; 0 sends nothing.")
    parser.add_argument("--once", action="store_true", help="Stop at the end of the first pass instead of looping.")
    args = parser.parse_args(argv)
    if not math.isfinite(args.rate) or args.rate < 0:
        parser.error("--rate must be finite and nonnegative")
    if args.limit is not None and args.limit < 0:
        parser.error("--limit must be nonnegative")
    if not args.topic.strip():
        parser.error("--topic must not be empty")
    try:
        events = read_events(
            args.input.expanduser().resolve(), load_state_codes(),
            limit=0 if args.limit == 0 else None,
        )
        if not events:
            print("No purchase events selected; nothing sent.")
            return 0
        mode = "one pass" if args.once else "continuous replay; Ctrl+C to stop"
        print(f"Loaded {len(events):,} purchases; base rate={args.rate:g}/s, profile={args.profile}, {mode}.", flush=True)
        sent = send_events(events, args.bootstrap_servers, args.topic, args.rate, args.profile,
                           loop=not args.once, limit=args.limit)
    except KeyboardInterrupt:
        print("Replay interrupted.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"Replay failed: {exc}", file=sys.stderr)
        return 1
    print(f"Sent {sent} purchase events to {args.topic} (rate={args.rate:g}, profile={args.profile}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
