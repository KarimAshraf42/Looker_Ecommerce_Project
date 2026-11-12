import argparse
import json
import random
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

kafka_bootstrap_servers = [
    "kafka1:19092",
    "kafka2:19093",
    "kafka3:19094",
]

topic = "events"

session_funnel = [
    ("Home", "/"),
    ("Department", "/department/{}"),
    ("Product", "/product/{}"),
    ("Cart", "/cart"),
    ("Purchase", "/purchase"),
]
cancel_step = ("Cancel", "/cancel")
cancel_rate = 0.2  

departments = ["men", "women"]
browsers = ["Chrome", "Firefox", "Safari", "IE", "Other"]
traffic_sources = ["Organic", "Adwords", "Email", "Facebook", "YouTube"]

cities = [
    ("Cairo", "Cairo"),
    ("Giza", "Giza"),
    ("Alexandria", "Alexandria"),
    ("Sharqia", "Sharqia"),
    ("Minya", "Minya"),
]

def create_producer(retries: int = 10):
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=kafka_bootstrap_servers,
                value_serializer=lambda v: (
                    v.encode("utf-8") if isinstance(v, str)
                    else json.dumps(v, default=str).encode("utf-8")
                ),
                key_serializer=lambda k: (
                    str(k).encode("utf-8") if k is not None else None
                ),
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=1,
            )
        except NoBrokersAvailable:
            print(
                f"[{attempt}/{retries}] Kafka unavailable at "
                f"{kafka_bootstrap_servers}; retrying in 3 seconds..."
            )
            time.sleep(3)

    raise RuntimeError(
        f"Could not connect to Kafka at {kafka_bootstrap_servers}"
    )


def on_send_error(exc):
    print(f"[ERROR] failed to send event to Kafka: {exc!r}")


def make_invalid(event: dict):
    """
    Returns (key, value) of a deliberately BROKEN copy of `event`.
    Every kind breaks one rule of the stream's valid_event() check, so each one must end in the
    dead-letter topic (events_dlq) and never in fact_events.
    """
    bad = dict(event)
    kind = random.choice(["no_id", "bad_sequence", "future", "bad_date", "not_json"])

    if kind == "no_id":
        bad["event_id"] = None
        return None, bad
    if kind == "bad_sequence":
        bad["sequence_number"] = random.choice([0, -1])
    elif kind == "future":
        bad["created_at"] = (
            datetime.now(timezone.utc) + timedelta(hours=2)
        ).isoformat(timespec="milliseconds")
    elif kind == "bad_date":
        bad["created_at"] = "not-a-date"
    else:
        return event["event_id"], "this-is-not-json {event_id=" + str(event["event_id"])
    return event["event_id"], bad

def kafka_simulation(
    producer,
    rate: int = 6,
    n_users: int = 6,
    dup_rate: float = 0.05,
    late_rate: float = 0.05,
    max_late_minutes: int = 5,
    very_late_rate: float = 0.02,
    very_late_minutes: int = 180,
    invalid_rate: float = 0.02,
):
    """
    Simulates a live clickstream.

    Every event is stamped with the real time it is sent, so the stream behaves
    like production traffic (and the Spark watermark keeps advancing normally).
    To exercise the pipeline's challenges on purpose:
      - dup_rate  : probability that an already-sent event is sent AGAIN (same event_id)
      - late_rate : probability that an event arrives LATE, i.e. its created_at is
                    1..max_late_minutes minutes in the past (inside the stream's 1 hour watermark,
                    so the stream still accepts it)
      - very_late_rate : probability that an event arrives VERY late, i.e. its created_at is
                    61..very_late_minutes minutes in the past (older than the 1 hour watermark, so
                    the stream drops it and late_streaming_events recovers it later)
      - invalid_rate : probability that an event is sent BROKEN on purpose (missing event_id,
                    sequence_number <= 0, future or unparseable created_at, or not JSON at all),
                    so it lands in the dead-letter topic
    """
    if rate <= 0:
        raise ValueError("rate must be greater than zero")
    if n_users <= 0:
        raise ValueError("n_users must be greater than zero")
    if not 0 <= dup_rate <= 1 or not 0 <= late_rate <= 1:
        raise ValueError("dup-rate and late-rate must be between 0 and 1")
    if max_late_minutes < 1:
        raise ValueError("max-late-minutes must be at least 1")
    if not 0 <= very_late_rate <= 1 or not 0 <= invalid_rate <= 1:
        raise ValueError("very-late-rate and invalid-rate must be between 0 and 1")
    if late_rate + very_late_rate > 1:
        raise ValueError("late-rate + very-late-rate must not be greater than 1")
    if very_late_minutes < 61:
        raise ValueError("very-late-minutes must be at least 61 (the stream's watermark is 1 hour)")

    print(
        f"Simulating approximately {rate} events/sec with {n_users} users "
        f"(duplicates: {dup_rate:.0%}, late events: {late_rate:.0%} "
        f"up to {max_late_minutes} min, very late events: {very_late_rate:.0%} "
        f"61-{very_late_minutes} min, invalid events: {invalid_rate:.0%}). "
        f"Press Ctrl+C to stop."
    )

    user_pool = [100 + i for i in range(n_users)]
    event_id = int(time.time() * 1000)
    recent_events = deque(maxlen=50) 
    while True:
        user_id = (
            None if random.random() < (1 / 6)
            else random.choice(user_pool)
        )

        session_id = str(uuid.uuid4())
        city, state = random.choice(cities)
        browser = random.choice(browsers)
        traffic_source = random.choice(traffic_sources)

        n_events = random.randint(1, len(session_funnel))
        steps = list(session_funnel[:n_events])

        if n_events == len(session_funnel) and random.random() < cancel_rate:
            steps[-1] = cancel_step

        for step, (event_type, uri) in enumerate(steps):
            if event_type == "Department":
                uri = uri.format(random.choice(departments))
            elif event_type == "Product":
                uri = uri.format(random.randint(1, 30000))

            event_time = datetime.now(timezone.utc)

            r = random.random()
            if r < late_rate:
                event_time -= timedelta(
                    seconds=random.randint(60, max_late_minutes * 60)
                )
            elif r < late_rate + very_late_rate:
                # older than the stream's 1 hour watermark
                event_time -= timedelta(
                    seconds=random.randint(61 * 60, very_late_minutes * 60)
                )

            event = {
                "event_id": event_id,
                "user_id": user_id,
                "sequence_number": step + 1,
                "session_id": session_id,
                "created_at": event_time.isoformat(timespec="milliseconds"),
                "city": city,
                "state": state,
                "browser": browser,
                "traffic_source": traffic_source,
                "uri": uri,
                "event_type": event_type,
            }

            if random.random() < invalid_rate:
                key, value = make_invalid(event)
                producer.send(topic, key=key, value=value).add_errback(on_send_error)
            else:
                producer.send(topic, key=event_id, value=event).add_errback(on_send_error)
                recent_events.append(event)
            event_id += 1

            if random.random() < dup_rate:
                dup = random.choice(recent_events)
                producer.send(topic, key=dup["event_id"], value=dup).add_errback(on_send_error)

        producer.flush()
        time.sleep(n_events / rate)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--rate", type=int, default=6)
    parser.add_argument("--users", type=int, default=6)
    parser.add_argument("--dup-rate", type=float, default=0.05,
                        help="probability of re-sending an already-sent event")
    parser.add_argument("--late-rate", type=float, default=0.05,
                        help="probability of an event arriving late")
    parser.add_argument("--max-late-minutes", type=int, default=5,
                        help="how far in the past a late event can be")
    parser.add_argument("--very-late-rate", type=float, default=0.02,
                        help="probability of an event older than the stream's 1 hour watermark")
    parser.add_argument("--very-late-minutes", type=int, default=180,
                        help="how far in the past a very late event can be (minimum 61)")
    parser.add_argument("--invalid-rate", type=float, default=0.02,
                        help="probability of sending a broken event (goes to the dead-letter topic)")

    args = parser.parse_args()

    producer = create_producer()
    print(f"Connected to Kafka at {kafka_bootstrap_servers}, topic '{topic}'")

    try:
        kafka_simulation(
            producer,
            rate=args.rate,
            n_users=args.users,
            dup_rate=args.dup_rate,
            late_rate=args.late_rate,
            max_late_minutes=args.max_late_minutes,
            very_late_rate=args.very_late_rate,
            very_late_minutes=args.very_late_minutes,
            invalid_rate=args.invalid_rate,
        )
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
