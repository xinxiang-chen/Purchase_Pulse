# Purchase Pulse

Real-time U.S. purchase analytics built with **Kafka, Spark Structured Streaming, Flask, and Socket.IO**. A reproducible synthetic event stream powers an interactive dashboard with age-band comparisons, a purchase-volume timeline, and a map of all 50 states.

The dashboard shows **only the current replay round**. When the producer finishes the dataset, it resets the displayed statistics and starts another round automatically.

## Features

- Purchase counts for ages 18-24, 25-34, 35-44, 45-54, and 55+.
- Purchase volume in five-second processing-time buckets, with one-minute and five-minute views.
- An interactive U.S. state map, including Alaska and Hawaii insets, and a top-five state leaderboard.
- Purchase-count and revenue views, plus current-round purchase count, revenue, average purchase value, and active states.
- Live updates without refreshing, responsive desktop and mobile layouts, and an English interface with locally bundled assets.
- Automatic round resets with protection against duplicate batches and delayed messages from older rounds.

All records are fictional. Counts represent **purchase events**, not distinct shoppers or distinct orders.

## Architecture

```text
Synthetic CSV
    |
    v
Python producer -- shopping-events --> Spark Structured Streaming
                                            |
                                     shopping-metrics
                                            |
                                            v
                                  Flask + SQLite store
                                            |
                                         Socket.IO
                                            |
                                            v
                                      Web dashboard
```

The producer sends a replay-start marker before each pass through the CSV. Spark publishes a `round_start` control record and per-round `purchases` aggregates. The web service applies them atomically to SQLite and sends dashboard snapshots to connected browsers.

## Requirements

- Python 3.11 and [uv](https://docs.astral.sh/uv/).
- Java 17.
- Docker Desktop, or a local Docker engine.
- macOS or Linux with Bash for the provided launcher.

The Python dependencies are pinned in [requirements.txt](requirements.txt), including PySpark 3.5.8. The launcher uses the matching Kafka connector. No separate Spark cluster or Hadoop service is required for this local demo.

## Quick start

Run setup commands from the repository root.

### 1. Install Python dependencies

```bash
uv venv --python 3.11
uv pip install -r requirements.txt
```

Commands below use `.venv/bin/python` directly, so activating the virtual environment is optional. On macOS, the Spark launcher selects Java 17 when `JAVA_HOME` is unset. On Linux, set `JAVA_HOME` or put Java 17 on `PATH`.

### 2. Start Kafka

Start Docker Desktop, then create the local Kafka container:

```bash
docker run -d --name dashboard-kafka \
  -p 127.0.0.1:9092:9092 apache/kafka:3.9.1
```

For an existing stopped container, use `docker start dashboard-kafka` instead. Once Kafka is ready, create the two topics:

```bash
for topic in shopping-events shopping-metrics; do
  docker exec dashboard-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$topic" \
    --partitions 1 --replication-factor 1
done
```

Both the producer and Spark publisher use partition 0 to preserve round ordering.

### 3. Generate the dataset

```bash
.venv/bin/python ingestion/generate_synthetic.py
```

This creates `data/user_log.csv` with **100,000 purchases**, seed `42`, 500 stable customer profiles, and all 50 states. CSV files are excluded from version control, so generate the dataset after a fresh checkout. If the file already exists, use it as-is or regenerate it explicitly:

```bash
.venv/bin/python ingestion/generate_synthetic.py --rows 100000 --seed 42 --force
```

Generation publishes the complete file atomically. See [data/README.md](data/README.md) for fields and validation rules.

### 4. Start the services

Keep each command running in a separate terminal, from the repository root.

**Terminal 1 - Spark**

```bash
bash scripts/start_streaming.sh
```

**Terminal 2 - Web service**

```bash
.venv/bin/python web/app.py
```

Open [http://127.0.0.1:5001](http://127.0.0.1:5001).

**Terminal 3 - Purchase producer**

```bash
.venv/bin/python ingestion/producer.py
```

The producer's default base rate is **5 purchases/second**. Its wave profile cycles through 2.5, 5, 7.5, and 5 purchases/second in ten-second phases. One pass through 100,000 records takes approximately 5 hours 33 minutes. It then starts the next round until interrupted.

### 5. Stop the services

Press `Ctrl+C` in the producer, Spark, and web terminals, then stop Kafka:

```bash
docker stop dashboard-kafka
```

For a normal restart, keep the existing Kafka container, checkpoint, and SQLite database. There is no need to recreate the environment or topics.

## Replay controls

| Option | Behavior |
| --- | --- |
| `--rate 2 --profile steady` | Send at a steady base rate of two purchases/second. |
| `--once` | Send one pass, then exit. |
| `--limit 100` | Stop after 100 acknowledged purchases across all rounds. |
| `--rate 0 --limit 100` | Run a finite, unpaced smoke test. |
| `--limit 0` | Validate the CSV header and exit without connecting to Kafka. |
| `--input data/small_demo.csv` | Replay another compatible CSV. |

Replay-start markers do not count toward `--limit` or purchase totals. A bounded or one-pass run keeps its final round visible when it stops.

To test a round boundary quickly:

```bash
.venv/bin/python ingestion/generate_synthetic.py --rows 5 --output data/small_demo.csv --force
.venv/bin/python ingestion/producer.py --input data/small_demo.csv --rate 1 --limit 12
```

The dashboard will progress through two complete five-purchase rounds and finish with two purchases in the third round.

## How round resets work

1. Each producer process creates a unique run ID. Every pass has its own round ID and fresh purchase-event IDs.
2. The producer sends an acknowledged `replay_start` marker before its purchases. The marker's Kafka offset establishes round order.
3. Spark splits microbatches at round boundaries, publishes round starts before purchases, and attaches round metadata to every metric record.
4. The web store atomically clears the preceding round's purchase count, revenue, age counts, state counts, and trend when a newer round arrives.
5. Duplicate batches within the current round count once. Delayed records from an older round are ignored and cannot restore cleared totals.

Refreshing the browser or restarting Spark/the web service preserves the current round. Starting a new producer process starts a new round. Kafka data and Spark checkpoint progress are kept; clearing a round does not require deleting either.

The trend uses **processing time** in five-second buckets. Source timestamps are synthetic and do not represent real shopping peaks. Amounts are transmitted as integer USD cents and formatted as dollars in the dashboard.

## Configuration

### Producer

```bash
.venv/bin/python ingestion/producer.py --help
```

Additional options include `--topic`, `--bootstrap-servers`, `--profile`, and `--input`.

### Streaming job

```bash
.venv/bin/python streaming/job.py --help
bash scripts/start_streaming.sh --available-now
.venv/bin/python scripts/inspect_results.py
```

The launcher uses the project virtual environment, `local[2]`, and `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8`. Connector dependencies are downloaded on first launch and cached under `runtime/ivy/`. Hadoop client libraries already included with Spark are excluded from duplicate downloads.

The default checkpoint is `runtime/checkpoints/us-shopping-v2/`. `--starting-offsets earliest|latest` applies only to a new checkpoint; normal restarts resume saved progress.

### Web service

| Environment variable | Default |
| --- | --- |
| `PORT` | `5001` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `SHOPPING_METRICS_TOPIC` | `shopping-metrics` |
| `DASHBOARD_DB` | `runtime/us-dashboard.sqlite3` |
| `DASHBOARD_SECRET` | Local development value |

For another local port:

```bash
PORT=5002 .venv/bin/python web/app.py
```

## Tests

```bash
.venv/bin/python -m unittest discover -s tests -v
```

The tests cover CSV validation, deterministic profiles, precise money conversion, replay pacing and limits, marker ordering, atomic round resets, duplicate handling, stale-round rejection, and database restart behavior. They use temporary databases and mocked Kafka clients; running this test suite does not require live Kafka or Spark services.

## Project layout

```text
config/us_states.json           Shared state codes and names
data/                          Generated CSV and dataset documentation
ingestion/generate_synthetic.py Reproducible data generation
ingestion/producer.py           Validated, rate-controlled replay
streaming/job.py                Spark round-aware aggregation
scripts/start_streaming.sh      Local Spark launcher
scripts/inspect_results.py      Kafka metrics inspector
scripts/english_assets.py       Reproducible English chart-asset patch
web/app.py                     HTTP, Socket.IO, and Kafka bridge
web/state.py                   Transactional current-round statistics
web/templates/                 Dashboard HTML
web/static/                    Styles, charts, map geometry, and licenses
tests/                         Automated regression tests
runtime/                       Ignored checkpoints, SQLite state, and caches
```

## Scope and data sources

This is a local demonstration with a single ordered Kafka input partition and local Spark workers. It does not claim transactional end-to-end exactly-once processing. Recreating input topics or deleting checkpoints can invalidate saved offset assumptions; use normal service restarts to continue a session.

The data is entirely synthetic and is not an estimate of U.S. demographics, spending, or customer behavior. The 50-state map is derived from the Apache ECharts example geometry. Alaska and Hawaii are shown as insets.

ECharts 5.6.0 is vendored with an English display-text patch. Upstream license and notice files are retained. Asset sources, local changes, and checksums are documented in [web/static/licenses/SOURCES.md](web/static/licenses/SOURCES.md) and [asset-manifest.json](web/static/licenses/asset-manifest.json).
