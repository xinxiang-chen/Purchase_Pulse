"""Durable current-round statistics with atomic, idempotent batch application."""

from datetime import datetime, timezone
from contextlib import contextmanager
from pathlib import Path
import sqlite3
from threading import RLock
import time


AGE_GROUPS = ("18–24", "25–34", "35–44", "45–54", "55+")
REPLAY_FIELDS = ("id", "number", "started_at", "source_topic", "start_offset")


def iso_timestamp(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")


def nonnegative_int(value):
    if type(value) is not int or value < 0:
        raise ValueError("Expected a nonnegative integer")
    return value


class DashboardStore:
    def __init__(self, database, catalog):
        self.database = Path(database)
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self.states = catalog
        self.codes = {state["code"] for state in catalog}
        self.lock = RLock()
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS batches (
                    batch_key TEXT PRIMARY KEY, processed_at REAL NOT NULL,
                    purchases INTEGER NOT NULL, revenue_cents INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ages (label TEXT PRIMARY KEY, count INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS states (
                    code TEXT PRIMARY KEY, count INTEGER NOT NULL, revenue_cents INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS active_replay (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    replay_id TEXT NOT NULL, number INTEGER NOT NULL,
                    started_at TEXT NOT NULL, source_topic TEXT NOT NULL,
                    start_offset INTEGER NOT NULL
                );
            """)
            # Legacy totals have no round identity and cannot be carried forward.
            db.execute("BEGIN IMMEDIATE")
            if self._active_replay(db) is None:
                self._clear_statistics(db)

    @staticmethod
    def _active_replay(db):
        row = db.execute(
            "SELECT replay_id, number, started_at, source_topic, start_offset "
            "FROM active_replay WHERE singleton=1"
        ).fetchone()
        return dict(zip(REPLAY_FIELDS, row)) if row is not None else None

    @staticmethod
    def _clear_statistics(db):
        for table in ("batches", "ages", "states"):
            db.execute(f"DELETE FROM {table}")

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(self.database, timeout=10)
        try:
            with db:
                yield db
        finally:
            db.close()

    def apply_batch(self, payload):
        if not isinstance(payload, dict) or payload.get("schema_version") != 2:
            raise ValueError("Unsupported metrics schema")
        kind = payload.get("type")
        if kind not in ("round_start", "purchases"):
            raise ValueError("Invalid metrics type")
        replay = payload.get("replay")
        if not isinstance(replay, dict):
            raise ValueError("Missing replay metadata")
        if not isinstance(replay.get("id"), str) or not replay["id"].strip() or len(replay["id"]) > 200:
            raise ValueError("Invalid replay ID")
        if type(replay.get("number")) is not int or replay["number"] < 1:
            raise ValueError("Invalid replay number")
        if not isinstance(replay.get("source_topic"), str) or not replay["source_topic"].strip():
            raise ValueError("Invalid replay source topic")
        nonnegative_int(replay.get("start_offset"))
        if replay["start_offset"] > 9223372036854775807:
            raise ValueError("Replay offset is outside the SQLite integer range")
        if not isinstance(replay.get("started_at"), str):
            raise ValueError("Invalid replay start timestamp")
        started = datetime.fromisoformat(replay["started_at"].replace("Z", "+00:00"))
        if started.tzinfo is None:
            raise ValueError("Replay start timestamp must include a time zone")
        key = payload.get("batch_key")
        if not isinstance(key, str) or not key or len(key) > 200:
            raise ValueError("Invalid batch key")
        if not isinstance(payload.get("processed_at"), str):
            raise ValueError("Invalid batch timestamp")
        processed = datetime.fromisoformat(payload["processed_at"].replace("Z", "+00:00"))
        if processed.tzinfo is None:
            raise ValueError("Batch timestamp must include a time zone")
        purchases = nonnegative_int(payload["totals"]["purchases"])
        revenue = nonnegative_int(payload["totals"]["revenue_cents"])
        ages = payload["age_groups"]
        states = payload["states"]
        if len(ages) != len(AGE_GROUPS) or {row["label"] for row in ages} != set(AGE_GROUPS):
            raise ValueError("Invalid age groups")
        if sum(nonnegative_int(row["count"]) for row in ages) != purchases:
            raise ValueError("Age totals do not match purchase total")
        if len({row["code"] for row in states}) != len(states) or any(row["code"] not in self.codes for row in states):
            raise ValueError("Invalid state codes")
        if sum(nonnegative_int(row["count"]) for row in states) != purchases:
            raise ValueError("State totals do not match purchase total")
        if sum(nonnegative_int(row["revenue_cents"]) for row in states) != revenue:
            raise ValueError("State amounts do not match revenue total")
        if kind == "round_start" and (purchases or revenue):
            raise ValueError("A round start must contain zero purchases and revenue")
        with self.lock, self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            active = self._active_replay(db)
            if active is not None:
                if replay["source_topic"] != active["source_topic"]:
                    raise ValueError("A different input topic requires a separate dashboard database")
                if replay["start_offset"] < active["start_offset"]:
                    return False
                if replay["start_offset"] == active["start_offset"] and replay["id"] != active["id"]:
                    raise ValueError("Conflicting replay IDs at the same source offset")
            if active is None or replay["start_offset"] > active["start_offset"]:
                self._clear_statistics(db)
                db.execute(
                    "INSERT INTO active_replay VALUES (1, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(singleton) DO UPDATE SET replay_id=excluded.replay_id, "
                    "number=excluded.number, started_at=excluded.started_at, "
                    "source_topic=excluded.source_topic, start_offset=excluded.start_offset",
                    tuple(replay[field] for field in REPLAY_FIELDS),
                )
            inserted = db.execute(
                "INSERT OR IGNORE INTO batches VALUES (?, ?, ?, ?)",
                (key, processed.timestamp(), purchases, revenue),
            ).rowcount
            if not inserted:
                return False
            db.executemany(
                "INSERT INTO ages VALUES (?, ?) ON CONFLICT(label) DO UPDATE SET count=count+excluded.count",
                [(row["label"], row["count"]) for row in ages],
            )
            db.executemany(
                "INSERT INTO states VALUES (?, ?, ?) ON CONFLICT(code) DO UPDATE SET count=count+excluded.count, revenue_cents=revenue_cents+excluded.revenue_cents",
                [(row["code"], row["count"], row["revenue_cents"]) for row in states],
            )
        return True

    def snapshot(self, now=None):
        now = time.time() if now is None else now
        last_bucket = int(now // 5) * 5
        first_bucket = last_bucket - 295
        with self.lock, self._connect() as db:
            db.execute("BEGIN")
            replay = self._active_replay(db)
            ages = dict(db.execute("SELECT label, count FROM ages"))
            states = {row[0]: row[1:] for row in db.execute("SELECT code, count, revenue_cents FROM states")}
            latest = db.execute("SELECT MAX(processed_at) FROM batches").fetchone()[0]
            buckets = {int(row[0]): row[1:] for row in db.execute(
                "SELECT CAST(processed_at/5 AS INTEGER)*5, SUM(purchases), SUM(revenue_cents) FROM batches WHERE processed_at>=? AND processed_at<? GROUP BY 1",
                (first_bucket, last_bucket + 5),
            )}
        purchases = sum(value[0] for value in states.values())
        revenue = sum(value[1] for value in states.values())
        return {
            "schema_version": 2,
            "replay": replay,
            "updated_at": iso_timestamp(latest) if latest is not None else None,
            "totals": {
                "purchases": purchases, "revenue": revenue / 100,
                "average_purchase": revenue / (100 * purchases) if purchases else 0,
                "active_states": sum(value[0] > 0 for value in states.values()),
            },
            "age_groups": [{"label": label, "count": ages.get(label, 0)} for label in AGE_GROUPS],
            "states": [{
                "code": state["code"], "name": state["name"],
                "count": states.get(state["code"], (0, 0))[0],
                "revenue": states.get(state["code"], (0, 0))[1] / 100,
            } for state in self.states],
            "trend": [{
                "timestamp": iso_timestamp(bucket),
                "purchases": buckets.get(bucket, (0, 0))[0],
                "revenue": buckets.get(bucket, (0, 0))[1] / 100,
            } for bucket in range(first_bucket, last_bucket + 1, 5)],
        }
