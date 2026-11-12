"""Local U.S. shopping dashboard and its live Kafka bridge."""

import json
import logging
import os
from pathlib import Path
from threading import Lock
import time

from flask import Flask, jsonify
from flask import render_template
from flask_socketio import SocketIO, emit
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from state import DashboardStore


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CATALOG = json.loads((PROJECT_ROOT / "config/us_states.json").read_text())
app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("DASHBOARD_SECRET", "local-shopping-demo")
socketio = SocketIO(app, async_mode="threading")
store = DashboardStore(os.environ.get("DASHBOARD_DB", PROJECT_ROOT / "runtime/us-dashboard.sqlite3"), CATALOG)
LOGGER = logging.getLogger(__name__)
consumer_task = None
task_lock = Lock()
status_lock = Lock()
connection = {"kafka": "connecting", "message": "Waiting for the live event stream."}


def dashboard_snapshot():
    snapshot = store.snapshot()
    with status_lock:
        snapshot["connection"] = dict(connection)
    return snapshot


def set_status(status, message):
    with status_lock:
        connection.update(kafka=status, message=message)
    socketio.emit("dashboard:update", dashboard_snapshot())


def consume_metrics():
    topic = os.environ.get("SHOPPING_METRICS_TOPIC", "shopping-metrics")
    while True:
        consumer = None
        try:
            set_status("connecting", "Connecting to the live event stream.")
            consumer = KafkaConsumer(
                topic, bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(","),
                group_id=None, enable_auto_commit=False, auto_offset_reset="earliest",
            )
            set_status("connected", "Connected to the live event stream.")
            last_emit = 0
            while True:
                changed = False
                for records in consumer.poll(timeout_ms=1000, max_records=100).values():
                    for record in records:
                        if record.value is None:
                            continue
                        try:
                            payload = json.loads(record.value.decode("utf-8"))
                            changed = store.apply_batch(payload) or changed
                        except (ValueError, KeyError, TypeError, UnicodeError, OverflowError) as error:
                            LOGGER.warning("Skipped invalid metrics record: %s", error)
                if changed or time.monotonic() - last_emit >= 5:
                    socketio.emit("dashboard:update", dashboard_snapshot())
                    last_emit = time.monotonic()
        except (KafkaError, OSError) as error:
            LOGGER.warning("Live stream connection failed: %s", error)
            set_status("error", "Live updates are unavailable. Reconnecting.")
            socketio.sleep(3)
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except KafkaError:
                    LOGGER.exception("Failed to close the live stream consumer")


def ensure_consumer_started():
    global consumer_task
    with task_lock:
        if consumer_task is None or not consumer_task.is_alive():
            consumer_task = socketio.start_background_task(consume_metrics)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/dashboard")
def dashboard_api():
    ensure_consumer_started()
    response = jsonify(dashboard_snapshot())
    response.headers["Cache-Control"] = "no-store"
    return response


@socketio.on("connect")
def on_connect(auth=None):
    ensure_consumer_started()
    emit("dashboard:update", dashboard_snapshot())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    logging.getLogger("kafka").setLevel(logging.WARNING)
    socketio.run(app, host="127.0.0.1", port=int(os.environ.get("PORT", "5001")),
                 debug=False, use_reloader=False, allow_unsafe_werkzeug=True)
