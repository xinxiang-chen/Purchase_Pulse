"""Print coherent U.S. shopping metric batches from Kafka."""

import argparse
import json
from kafka import KafkaConsumer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", default="shopping-metrics")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    args = parser.parse_args()
    consumer = KafkaConsumer(args.topic, bootstrap_servers=args.bootstrap_servers.split(","),
                             group_id=None, enable_auto_commit=False, auto_offset_reset="latest")
    try:
        for record in consumer:
            if record.value is not None:
                print(json.dumps(json.loads(record.value), indent=2, ensure_ascii=False))
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
