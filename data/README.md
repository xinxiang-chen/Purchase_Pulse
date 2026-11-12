# Fictional US shopping data

The default demo dataset contains **100,000 fictional purchase events**, generated with seed `42`. It contains no customer data and makes no claim about US population, spending, or shopping behavior.

## Generate demo data

From the project root:

```bash
.venv/bin/python ingestion/generate_synthetic.py
```

The default output is `data/user_log.csv`. Existing files are protected; use `--force` to replace one:

```bash
.venv/bin/python ingestion/generate_synthetic.py --rows 100000 --seed 42 --force
.venv/bin/python ingestion/generate_synthetic.py --rows 100 --output data/small_demo.csv
```

Identical arguments and the same state catalog reproduce the same CSV. The generator writes a complete temporary file in the output directory, then publishes it atomically; a failed generation leaves the previous dataset intact. CSV files under `data/` are excluded from Git; the generator and catalog are the reproducible source.

## CSV fields

```text
event_id,user_id,age,state,amount,event_time
```

- `event_id`: unique fictional purchase identifier within the generated dataset.
- `user_id`: fictional user identifier; each user keeps the same age and state across all their purchases.
- `age`: integer from 18 through 80. The dashboard groups ages into `18–24`, `25–34`, `35–44`, `45–54`, and `55+`.
- `state`: two-letter postal code from `config/us_states.json`. All 50 states, including Alaska and Hawaii, appear when at least 50 events are generated. The catalog excludes DC and territories. The first five events also cover all five age groups.
- `amount`: positive USD value with exactly two decimal places. The invented price mixture ranges from `$5.00` to `$499.99`; it is not an estimate of real purchases.
- `event_time`: fictional UTC timestamp beginning at `2026-01-01T00:00:00.000Z`, advancing by 50 milliseconds per event. Replay pacing is independent of these source timestamps.

The CSV is UTF-8 with a header. Columns are read by name with `csv.DictReader`, so column order is not significant. Old CSVs containing the removed fields are rejected with a regeneration instruction.

The state catalog follows the [USPS two-letter abbreviations](https://pe.usps.com/text/pub28/28apb.htm), restricted to the 50 states.

## Replay to Kafka

With Kafka and the dashboard pipeline running:

```bash
.venv/bin/python ingestion/producer.py
```

The default base rate is 5 events/second. The default `wave` profile cycles through `0.5×`, `1×`, `1.5×`, and `1×` base rate in ten-second phases, equivalent to 2.5 / 5 / 7.5 / 5 events per second. The wave clock and send pacing continue across the end of each pass. `--profile steady` uses the specified rate throughout. These pacing choices are invented demo settings, not real traffic patterns.

At the end of the file, the producer returns to its first record and loops until interrupted with Ctrl+C. A pass through 100,000 rows takes approximately 5 hours 33 minutes at the default base rate. Each pass begins a new replay round. Purchase count, revenue, age groups, state totals and the trend reset when the new round starts. Kafka topics and Spark checkpoints stay intact.

```bash
.venv/bin/python ingestion/producer.py --once
.venv/bin/python ingestion/producer.py --rate 2 --profile steady
.venv/bin/python ingestion/producer.py --rate 0 --limit 100
.venv/bin/python ingestion/producer.py --input data/small_demo.csv --topic shopping-events
```

Use `--bootstrap-servers` to change the default `localhost:9092`. `--limit` caps the total acknowledged sends across all passes; `--once --limit N` stops after the first pass or N sends, whichever comes first. `--limit 0` validates the header and sends no records. Otherwise the whole input is validated before connecting to Kafka; duplicate source event IDs or changing age/state for the same user are errors. Empty input exits without connecting or spinning in an empty loop.

The producer first sends an acknowledged `replay_start` control record to partition 0 of `shopping-events`. Its Kafka offset is the round order. Purchase records on that same partition include this metadata:

```json
{
  "schema_version": 2,
  "event_type": "purchase",
  "replay_id": "<producer-run-id>:1",
  "replay_number": 1,
  "replay_started_at": "2026-11-08T12:00:00.000Z",
  "replay_start_offset": 12345,
  "event_id": "demo-purchase-42-00000001:<producer-run-id>:1",
  "user_id": "demo-user-00001",
  "age": 21,
  "state": "AL",
  "amount_cents": 1599,
  "event_time": "2026-01-01T00:00:00.000Z",
  "ingested_at": "2026-11-08T12:00:00.000Z"
}
```

The example values are illustrative. `amount` is converted to integer cents with `Decimal`, avoiding floating-point money errors. `ingested_at` is set to the current UTC time immediately before sending. Every send awaits its Kafka acknowledgement; pending sends are flushed and the producer is closed on exit. Replay-start control records are excluded from `--limit` and purchase statistics.

Each transmitted `event_id` includes the source purchase ID, a unique producer-run ID, and the pass number. IDs are fresh across passes and producer restarts; each replay is treated as new demo purchases. `user_id`, age, state, amount, and source `event_time` stay fixed, while `ingested_at` is refreshed for every send. The CSV itself is unchanged by replaying it. A new round clears the dashboard statistics for the preceding round. `--once` and `--limit` stop without announcing an extra empty round, so the last round remains visible.
