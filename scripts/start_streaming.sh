#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_PYTHON="$PROJECT_ROOT/.venv/bin/python"
SPARK_SUBMIT="$PROJECT_ROOT/.venv/bin/spark-submit"

if [[ ! -x "$PROJECT_PYTHON" || ! -x "$SPARK_SUBMIT" ]]; then
    printf 'Create the project .venv and install pyspark==3.5.8 and kafka-python first.\n' >&2
    exit 1
fi

# Use Spark and Python from this project, including when another Spark is installed.
SPARK_HOME="$("$PROJECT_PYTHON" - <<'PY'
from pathlib import Path
import pyspark

if pyspark.__version__ != "3.5.8":
    raise SystemExit("This launcher requires pyspark==3.5.8 to match its Kafka connector.")
print(Path(pyspark.__file__).resolve().parent)
PY
)"
export SPARK_HOME
export PYSPARK_PYTHON="$PROJECT_PYTHON"
export PYSPARK_DRIVER_PYTHON="$PROJECT_PYTHON"
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"

if [[ -z "${JAVA_HOME:-}" && -x /usr/libexec/java_home ]]; then
    JAVA_HOME="$(/usr/libexec/java_home -v 17)"
    export JAVA_HOME
fi

exec "$SPARK_SUBMIT" \
    --master 'local[2]' \
    --conf spark.sql.shuffle.partitions=2 \
    --conf "spark.jars.ivy=$PROJECT_ROOT/runtime/ivy" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
    --exclude-packages org.apache.hadoop:hadoop-client-api,org.apache.hadoop:hadoop-client-runtime \
    "$PROJECT_ROOT/streaming/job.py" \
    --bootstrap-servers "${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}" \
    "$@"
