"""Sends fake order events to Kafka continuously."""
import json
import os
import random
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from event_gen import generate_order

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "orders")
RATE = float(os.getenv("EVENTS_PER_SEC", "2"))
BAD_PROB = 0.02  # 2% invalid records to demo data-quality filtering


def connect():
    for attempt in range(1, 31):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
            )
        except NoBrokersAvailable:
            print("Kafka not ready (attempt %d/30), retrying..." % attempt)
            time.sleep(3)
    raise SystemExit("Could not connect to Kafka")


def main():
    producer = connect()
    print("Producing to topic '%s' at %.1f events/sec" % (TOPIC, RATE))
    sent = 0
    while True:
        event = generate_order(bad=random.random() < BAD_PROB)
        producer.send(TOPIC, event)
        sent += 1
        if sent % 20 == 0:
            print("sent %d events" % sent)
        time.sleep(1.0 / RATE)


if __name__ == "__main__":
    main()
