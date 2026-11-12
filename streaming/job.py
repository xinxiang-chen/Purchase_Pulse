"""Aggregate adult U.S. purchase events with Spark Structured Streaming."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import logging
from pathlib import Path

from kafka import KafkaProducer
from pyspark.sql import SparkSession, functions as F, types as T


PROJECT_ROOT = Path(__file__).resolve().parents[1]
AGE_GROUPS = ("18–24", "25–34", "35–44", "45–54", "55+")
LOGGER = logging.getLogger(__name__)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--input-topic", default="shopping-events")
    parser.add_argument("--metrics-topic", default="shopping-metrics")
    parser.add_argument("--checkpoint-dir", type=Path, default=PROJECT_ROOT / "runtime/checkpoints/us-shopping-v2")
    parser.add_argument("--starting-offsets", choices=("earliest", "latest"), default="earliest")
    parser.add_argument("--trigger-interval", default="1 second")
    parser.add_argument("--available-now", action="store_true", help="Process available input, then exit.")
    return parser.parse_args(argv)


def decode_events(messages):
    catalog = json.loads((PROJECT_ROOT / "config/us_states.json").read_text())
    state_codes = [state["code"] for state in catalog]
    schema = T.StructType([
        T.StructField("schema_version", T.IntegerType()),
        T.StructField("event_id", T.StringType()),
        T.StructField("user_id", T.StringType()),
        T.StructField("age", T.IntegerType()),
        T.StructField("state", T.StringType()),
        T.StructField("amount_cents", T.LongType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("replay_id", T.StringType()),
        T.StructField("replay_number", T.IntegerType()),
        T.StructField("replay_started_at", T.StringType()),
        T.StructField("replay_start_offset", T.LongType()),
    ])
    events = messages.select(
        F.from_json(F.col("value").cast("string"), schema).alias("event"),
        F.col("topic").alias("_topic"), F.col("partition").alias("_partition"),
        F.col("offset").alias("_offset"),
    ).select("event.*", "_topic", "_partition", "_offset").withColumn(
        "replay_start_offset",
        F.when(F.col("event_type") == "replay_start", F.col("_offset"))
        .otherwise(F.col("replay_start_offset")),
    )
    round_valid = (
        (F.col("_partition") == 0)
        & (F.length(F.trim(F.col("replay_id"))) > 0)
        & (F.col("replay_number") >= 1)
        & (F.length(F.trim(F.col("replay_started_at"))) > 0)
        & (F.col("replay_start_offset") >= 0)
    )
    purchase_valid = (
        (F.col("event_type") == "purchase")
        & (F.col("replay_start_offset") < F.col("_offset"))
        & (F.length(F.trim(F.col("event_id"))) > 0)
        & (F.length(F.trim(F.col("user_id"))) > 0)
        & F.col("age").between(18, 100)
        & F.col("state").isin(state_codes)
        & (F.col("amount_cents") >= 0)
    )
    valid = events.where(
        (F.col("schema_version") == 2)
        & round_valid
        & ((F.col("event_type") == "replay_start") | purchase_valid)
    )
    return valid.withColumn(
        "age_group",
        F.when(F.col("event_type") != "purchase", F.lit(None).cast("string"))
        .when(F.col("age") < 25, AGE_GROUPS[0])
        .when(F.col("age") < 35, AGE_GROUPS[1])
        .when(F.col("age") < 45, AGE_GROUPS[2])
        .when(F.col("age") < 55, AGE_GROUPS[3])
        .otherwise(AGE_GROUPS[4]),
    )


def publish_round_records(records, batch_id, producer, metrics_topic, replay, kind):
    counts = {label: 0 for label in AGE_GROUPS}
    totals = {"purchases": 0, "revenue_cents": 0}
    states = []
    if kind == "purchases":
        totals = records.agg(
            F.count("*").alias("purchases"), F.sum("amount_cents").alias("revenue_cents")
        ).first().asDict()
        for row in records.groupBy("age_group").count().collect():
            counts[row["age_group"]] = row["count"]
        states = [row.asDict() for row in (
            records.groupBy(F.col("state").alias("code"))
            .agg(F.count("*").alias("count"), F.sum("amount_cents").alias("revenue_cents"))
            .orderBy("code").collect()
        )]
    ranges = [row.asDict() for row in (
        records.groupBy("_topic", "_partition")
        .agg(F.min("_offset").alias("first"), F.max("_offset").alias("last"), F.count("*").alias("count"))
        .orderBy("_topic", "_partition").collect()
    )]
    batch_key = "us-round-" + hashlib.sha256(json.dumps(
        {"replay_id": replay["id"], "kind": kind, "ranges": ranges},
        sort_keys=True, separators=(",", ":"),
    ).encode()).hexdigest()
    payload = {
        "schema_version": 2, "batch_key": batch_key, "batch_id": batch_id,
        "processed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "type": kind, "replay": replay, "totals": totals,
        "age_groups": [{"label": label, "count": counts[label]} for label in AGE_GROUPS],
        "states": states,
    }
    producer.send(metrics_topic, partition=0, key=batch_key.encode(), value=payload).get(timeout=30)
    LOGGER.info("Published round %s, batch %s: %s purchases", replay["number"], batch_id, totals["purchases"])


def publish_metrics(batch, batch_id, producer, metrics_topic):
    """Split boundary-crossing batches and apply round starts in source order."""
    batch.persist()
    try:
        if batch.isEmpty():
            return
        rounds = batch.select(
            "_topic", "replay_id", "replay_number", "replay_started_at", "replay_start_offset"
        ).distinct().orderBy("replay_start_offset", "replay_id").collect()
        for row in rounds:
            records = batch.where(
                (F.col("_topic") == row["_topic"])
                & (F.col("replay_id") == row["replay_id"])
                & (F.col("replay_start_offset") == row["replay_start_offset"])
            )
            replay = {"id": row["replay_id"], "number": row["replay_number"],
                      "started_at": row["replay_started_at"], "source_topic": row["_topic"],
                      "start_offset": row["replay_start_offset"]}
            kinds = {item["event_type"] for item in records.select("event_type").distinct().collect()}
            if "replay_start" in kinds:
                publish_round_records(records.where(F.col("event_type") == "replay_start"),
                                      batch_id, producer, metrics_topic, replay, "round_start")
            if "purchase" in kinds:
                publish_round_records(records.where(F.col("event_type") == "purchase"),
                                      batch_id, producer, metrics_topic, replay, "purchases")
    finally:
        batch.unpersist()


def main(argv=None):
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    logging.getLogger("kafka").setLevel(logging.WARNING)
    spark = SparkSession.builder.appName("USShoppingAnalytics").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    producer = query = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers.split(","), acks="all", retries=3,
            value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        )
        messages = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.input_topic)
            .option("startingOffsets", args.starting_offsets).load()
        )
        checkpoint = args.checkpoint_dir.expanduser().resolve()
        checkpoint.mkdir(parents=True, exist_ok=True)
        writer = (
            decode_events(messages).writeStream
            .foreachBatch(lambda batch, batch_id: publish_metrics(batch, batch_id, producer, args.metrics_topic))
            .option("checkpointLocation", str(checkpoint)).queryName("us-shopping-analytics")
        )
        writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime=args.trigger_interval)
        query = writer.start()
        LOGGER.info("Reading %s; publishing %s", args.input_topic, args.metrics_topic)
        query.awaitTermination()
    except KeyboardInterrupt:
        LOGGER.info("Stopping streaming job")
    finally:
        try:
            if query is not None and query.isActive:
                query.stop()
        finally:
            try:
                if producer is not None:
                    producer.close(timeout=30)
            finally:
                spark.stop()


if __name__ == "__main__":
    main()
