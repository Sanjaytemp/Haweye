"""The generator is the *contract boundary* of the whole project: if its payload
drifts from `jobs/common/schema.py`, every downstream job silently degrades
(NULL features, no errors).  These tests pin the contract.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from common import schema
from generator import TransactionSimulator, build_cards, build_merchants, generate_transactions
from generator.generator import to_payload

WIRE_FIELDS = set(schema.TRANSACTION_SCHEMA.fieldNames())


@pytest.fixture(scope="module")
def sim():
    rng = __import__("random").Random(99)
    return TransactionSimulator(build_merchants(40, rng), build_cards(60, rng),
                                seed=99, fraud_rate=0.3,
                                start=datetime(2024, 6, 1, tzinfo=timezone.utc),
                                end=datetime(2024, 6, 1, 0, 10, tzinfo=timezone.utc))


def test_payload_parses_against_the_spark_schema(sim):
    """`is_valid_payload` is the same check `parse_transactions` applies."""
    for txn, label in sim.stream(200):
        payload = to_payload(txn, label)
        ok, errors = schema.is_valid_payload(payload)
        assert ok, f"contract violation {errors}: {payload}"


def test_wire_payload_never_leaks_ground_truth(sim):
    """THE most important test in the file.

    `label`/`velocity_5min`/`first_merchant_ever` are what the *simulator* knows.
    Shipping them to Kafka would give the model the answer key (label leakage)
    and would make any AUC here meaningless.
    """
    txn, label = sim.one()
    payload = to_payload(txn, label, include_label=False)
    leaked = {"label", "fraud_type", "score", "reasons", "velocity_5min",
              "first_merchant_ever", "_fraud_kind", "event_ts_ts"}
    assert not (set(payload) & leaked), set(payload) & leaked
    assert set(payload) <= WIRE_FIELDS | {"currency", "device_id"}


def test_labels_are_only_available_on_the_optional_side_channel(sim):
    txn, label = sim.one()
    withlabel = to_payload(txn, label, include_label=True)
    assert withlabel["label"] in (0, 1)
    assert withlabel["transaction_id"] == txn["transaction_id"]


def test_transaction_ids_unique_over_a_long_stream():
    txns, labels = generate_transactions(datetime(2024, 5, 1, tzinfo=timezone.utc),
                                         datetime(2024, 5, 1, 2, tzinfo=timezone.utc),
                                         n=2000, seed=5, fraud_rate=0.05)
    ids = [t["transaction_id"] for t in txns]
    assert len(set(ids)) == len(ids), "duplicates would be dropped by dedupe() and look like data loss"
    assert len({row["transaction_id"] for row in labels}) == len(ids)


def test_timestamps_are_iso8601_with_tz_and_ordered():
    start = datetime(2024, 5, 1, tzinfo=timezone.utc)
    txns, _ = generate_transactions(start, start + timedelta(hours=1), n=400, seed=11)
    stamps = []
    for t in txns:
        parsed = datetime.fromisoformat(t["event_ts"].replace("Z", "+00:00"))
        assert parsed.tzinfo is not None, "naive timestamps get reinterpreted per-node timezone"
        stamps.append(parsed)
    assert stamps == sorted(stamps), "event time must be monotone-ish or watermarks become nonsense"


def test_fraud_rate_actually_moves_the_label_rate():
    """A 'simulator' whose --fraud-rate does nothing is a trap for the next person."""
    start = datetime(2024, 5, 1, tzinfo=timezone.utc)
    end = start + timedelta(hours=3)
    rates = []
    for target in (0.02, 0.25):
        _, labels = generate_transactions(start, end, n=1200, seed=3, fraud_rate=target)
        rates.append(sum(row["label"] for row in labels) / len(labels))
    assert rates[0] < rates[1], rates
    # The *labelled* rate always undershoots the injected one on purpose
    # (label_noise + disputes), so assert direction, not equality.
    assert rates[1] > 0.10, f"even the fraud-heavy setting produced {rates[1]:.1%} labels"


def test_amounts_are_positive_and_currency_stable(sim):
    for txn, _ in sim.stream(300):
        assert txn["amount"] > 0
        assert txn["currency"] == "USD"
        assert txn["channel"] in {"online", "pos", "atm", "moto", "wallet"}
        assert isinstance(txn["card_present"], bool)


def test_country_codes_are_two_letters_uppercase(sim):
    """text (not char(2)) on the Postgres side + 2-letter here: the join must work."""
    for txn, _ in sim.stream(400):
        cc = txn["merchant_country"]
        assert isinstance(cc, str) and len(cc) == 2 and cc == cc.upper()


def test_generator_countries_are_a_subset_of_the_contract():
    """Drift guard between the producer and the consumer's allow-list."""
    from generator.simulator import COUNTRIES, HIGH_RISK_COUNTRIES

    emitted = set(COUNTRIES) | set(HIGH_RISK_COUNTRIES)
    assert emitted <= set(schema.COUNTRIES), emitted - set(schema.COUNTRIES)


def test_payload_is_json_serialisable_and_keyed_for_kafka(sim):
    txn, label = sim.one()
    payload = to_payload(txn, label)
    text = json.dumps(payload)                    # no datetime/Decimal surprises
    assert json.loads(text) == payload
    assert payload["card_id"].startswith("CARD-")  # the partition key: one card = one partition


def test_merchant_and_card_catalogues_are_consistent():
    rng = __import__("random").Random(4)
    merchants = build_merchants(120, rng)
    cards = build_cards(200, rng)
    assert len(merchants) == 120 and len(cards) == 200
    assert len({m["merchant_id"] for m in merchants}) == 120
    assert len({c["card_id"] for c in cards}) == 200
    for c in cards:
        assert c["credit_limit"] > 0 and c["txn_limit_1h"] > 0
    for m in merchants:
        assert 0 <= m["merchant_risk_score"] <= 1


def test_profile_column_lists_match_the_dimension_spec():
    """`generator/profiles.py` reads the dimension tables with its own column list
    (its container cannot import `jobs/common`), so nothing but this test stops it
    reading `merchant_risk_score` from the column that used to be `merchant_tier`."""
    from common import dimensions
    from generator import profiles

    for name, cols in (("merchants", profiles.MERCHANT_COLUMNS), ("card_accounts", profiles.CARD_COLUMNS)):
        # `updated_at` is deliberately absent: the generator wants the business
        # state, and the CDC job is the only thing that cares when it changed.
        spec = [c[0] for c in dimensions.TABLES[name]["columns"] if c[0] != "updated_at"]
        assert cols == spec, f"{name}: generator reads {cols}, dimension spec is {spec}"
