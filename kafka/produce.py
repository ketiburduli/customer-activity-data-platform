#!/usr/bin/env python3
"""
Optional: replay each sample file onto a Kafka topic named after it.

  pam_customers.json     -> topic "pam_customers"
  app_customers.json     -> topic "app_customers"
  activity_events.json   -> topic "activity_events"
  customer_sessions.json -> topic "customer_sessions"

Each line of the file is sent verbatim as one message value (raw JSON bytes),
so the planted edge cases survive intact. Run via:

  docker compose --profile kafka run --rm producer

This is entirely optional — the default path is to read ./sample-data directly.
"""
import glob
import os
import sys
import time

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

BOOTSTRAP = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
DATA_DIR = os.environ.get("DATA_DIR", "/sample-data")


def connect_producer(retries=20, delay=3):
    last = None
    for _ in range(retries):
        try:
            return KafkaProducer(bootstrap_servers=BOOTSTRAP, linger_ms=20)
        except NoBrokersAvailable as e:
            last = e
            print(f"  broker not ready, retrying in {delay}s...")
            time.sleep(delay)
    raise SystemExit(f"Could not reach Kafka at {BOOTSTRAP}: {last}")


def ensure_topics(topics):
    try:
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    except NoBrokersAvailable:
        return  # auto-create is enabled on the broker as a fallback
    new = [NewTopic(name=t, num_partitions=1, replication_factor=1) for t in topics]
    try:
        admin.create_topics(new)
    except TopicAlreadyExistsError:
        pass
    except Exception as e:
        print(f"  (topic create skipped: {e})")
    finally:
        admin.close()


def main():
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.json")))
    if not files:
        sys.exit(f"No *.json files found in {DATA_DIR}")

    topics = [os.path.splitext(os.path.basename(f))[0] for f in files]
    ensure_topics(topics)

    producer = connect_producer()
    grand = 0
    for path in files:
        topic = os.path.splitext(os.path.basename(path))[0]
        n = 0
        with open(path, "rb") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                producer.send(topic, value=line)
                n += 1
        producer.flush()
        grand += n
        print(f"  {topic}: produced {n} messages")
    producer.close()
    print(f"Done. {grand} messages across {len(files)} topics on {BOOTSTRAP}.")


if __name__ == "__main__":
    main()
