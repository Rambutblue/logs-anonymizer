"""Sends AMOUNT Cap'n Proto encoded log records to Kafka at about RATE records per second."""
import os
import sys
import time

import capnp
from confluent_kafka import Producer

from common import AMOUNT, TOPIC, record

RATE = int(os.environ.get("RATE", "500"))
BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9093")

capnp.remove_import_hook()
http_log = capnp.load("http_log.capnp")

failures = []


def on_delivery(err, msg):
    if err is not None:
        failures.append(err)


def encode(i):
    message = http_log.HttpLogRecord.new_message()
    for field, value in record(i).items():
        setattr(message, field, value)
    return message.to_bytes()


def main():
    # acks=all + idempotence: the broker confirms every record exactly once, retrying across broker restarts
    producer = Producer({
        "bootstrap.servers": BROKERS,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 5,
    })

    print(f"producing {AMOUNT} records at ~{RATE}/s to {TOPIC}", flush=True)
    start = time.monotonic()
    for i in range(1, AMOUNT + 1):
        value = encode(i)
        while True:
            try:
                producer.produce(TOPIC, value, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.5)
        producer.poll(0)

        ahead = start + i / RATE - time.monotonic()
        if ahead > 0:
            time.sleep(ahead)

    producer.flush()
    if failures:
        print(f"{len(failures)} records were not delivered, first error: {failures[0]}", flush=True)
        sys.exit(1)
    print(f"done, delivered {AMOUNT} records in {time.monotonic() - start:.1f}s", flush=True)


main()
