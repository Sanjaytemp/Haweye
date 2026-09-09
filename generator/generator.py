#!/usr/bin/env python3
"""Stream the synthetic acquirer feed into Kafka (`raw_transactions`).

This is the "external source" of the architecture.  It exists as a tiny,
dependency-light python container instead of requiring NiFi, because a project
you cannot run in five minutes teaches nothing.  NiFi is still supported and
documented (nifi/README.md) - the payload this script produces is exactly what
the NiFi flow produces.

    python generator.py --bootstrap-servers localhost:9094 --rate-per-sec 25
    python generator.py --duration 300 --speed-up 20 --card CARD-00042
    python generator.py --print 3          # no Kafka needed: just show payloads

Every record:  key = card_id (so one card always lands on one partition and the
streaming aggregations can be done per partition), value = compact JSON.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:                                # `generator` package (tests, IDE, scripts)
    from .profiles import load_profiles
    from .simulator import TransactionSimulator
except ImportError:                 # flat module layout inside the container
    from profiles import load_profiles  # noqa: E402
    from simulator import TransactionSimulator  # noqa: E402

PAYLOAD_FIELDS = (
    "transaction_id", "event_ts", "card_id", "merchant_id", "amount", "currency",
    "channel", "merchant_country", "card_present", "merchant_category", "is_3ds",
    "device_id",
)


def to_payload(txn: dict, label: dict | None = None, include_label: bool = False) -> dict:
    """Only the contract's fields go on the wire; labels never travel with events.

    `velocity_5min` / `first_merchant_ever` stay off the wire on purpose: the
    pipeline must compute those itself, not be told them by the producer.
    """
    payload = {k: txn[k] for k in PAYLOAD_FIELDS if k in txn}
    if include_label and label is not None:      # demo/backfill only!
        payload["label"] = int(label["label"])
        payload["fraud_type"] = label.get("fraud_type")
    return payload


class KafkaProducer:
    """Thin wrapper so `--print` works without confluent-kafka installed."""

    def __init__(self, bootstrap: str, topic: str):
        from confluent_kafka import Producer  # imported lazily

        self.topic = topic
        self.producer = Producer({
            "bootstrap.servers": bootstrap,
            "acks": "1",
            "linger.ms": 20,
            "batch.num.messages": 500,
            "compression.type": "lz4",
            "enable.idempotence": "true",
            "client.id": "haweye-generator",
        })

    def send(self, key: str, value: dict) -> None:
        self.producer.produce(self.topic, key=key.encode(),
                              value=json.dumps(value, separators=(",", ":")).encode())
        self.producer.poll(0)

    def flush(self) -> None:
        self.producer.flush(15)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="haweye transaction generator")
    ap.add_argument("--bootstrap-servers", default=os.environ.get("KAFKA_SERVERS", "localhost:9094"))
    ap.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC_RAW", "raw_transactions"))
    ap.add_argument("--rate-per-sec", type=float, default=float(os.environ.get("GEN_RATE_PER_SEC", 25)))
    ap.add_argument("--fraud-rate", type=float, default=float(os.environ.get("GEN_FRAUD_RATE", 0.05)))
    ap.add_argument("--duration", type=float, default=0.0, help="seconds (0 = forever)")
    ap.add_argument("--count", type=int, default=0, help="stop after N records")
    ap.add_argument("--speed-up", type=float, default=1.0,
                    help="compress event time: 20 = 20x faster than real time")
    ap.add_argument("--seed", type=int, default=int(os.environ.get("GEN_SEED", "20240601")))
    ap.add_argument("--n-cards", type=int, default=int(os.environ.get("GEN_CARDS", 200)))
    ap.add_argument("--n-merchants", type=int, default=int(os.environ.get("GEN_MERCHANTS", 120)))
    ap.add_argument("--synthetic", action="store_true",
                    help="do not read dimensions from Postgres (use generated profiles)")
    ap.add_argument("--card", default=None, help="pin one card (nice for watching features evolve)")
    ap.add_argument("--emit-labels", action="store_true",
                    help="put the ground-truth label in the payload (demo only)")
    ap.add_argument("--print", dest="print_n", type=int, default=0, help="print N payloads and exit")
    ap.add_argument("--dry-run", action="store_true", help="generate but do not send")
    args = ap.parse_args(argv)

    rng_seed = args.seed
    merchants, cards, source = load_profiles(rng_seed, args.n_merchants, args.n_cards,
                                             prefer_db=not args.synthetic)
    print(f">>> dimension profiles: {len(merchants)} merchants / {len(cards)} cards ({source})",
          flush=True)

    if args.print_n:
        sim = TransactionSimulator(merchants, cards, seed=rng_seed, fraud_rate=args.fraud_rate)
        for _ in range(args.print_n):
            txn, label = sim.one()
            print(json.dumps(to_payload(txn, label, args.emit_labels), indent=2, default=str))
        return 0

    now = datetime.now(timezone.utc).replace(microsecond=0)
    sim = TransactionSimulator(
        merchants, cards, seed=rng_seed, fraud_rate=args.fraud_rate,
        start=now, end=now + timedelta(days=365), rate_per_sec=args.rate_per_sec,
    )
    producer = None
    if not args.dry_run:
        try:
            producer = KafkaProducer(args.bootstrap_servers, args.topic)
        except ImportError:
            print("!!! confluent-kafka not installed: pip install confluent-kafka (or use --dry-run)",
                  file=sys.stderr)
            return 3

    stop = {"flag": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))

    period = 1.0 / max(0.05, args.rate_per_sec)
    event_clock = {"t": now}
    started = time.time()
    sent = fraud = 0
    next_report = started + 10
    print(f">>> producing to {args.topic} via {args.bootstrap_servers} @ {args.rate_per_sec}/s "
          f"(fraud-rate={args.fraud_rate}, speed-up={args.speed_up}); Ctrl-C to stop", flush=True)
    while not stop["flag"]:
        if args.duration and time.time() - started > args.duration:
            break
        if args.count and sent >= args.count:
            break
        event_clock["t"] += timedelta(seconds=period * args.speed_up)
        txn, label = sim.one(card_id=args.card, now=event_clock["t"])
        payload = to_payload(txn, label, args.emit_labels)
        if producer is not None:
            producer.send(txn["card_id"], payload)
        sent += 1
        fraud += int(label["label"])
        if time.time() >= next_report:
            print(f"    sent={sent} labelled_fraud={fraud} rate={fraud / max(sent, 1):.3%}", flush=True)
            next_report = time.time() + 10
        time.sleep(max(0.0, period - (time.time() - started - sent * period)) if period else 0)
    if producer is not None:
        producer.flush()
    print(f">>> done: {sent} records ({fraud} labelled fraud)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
