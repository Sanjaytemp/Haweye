"""The synthetic acquirer — the "external source" of this architecture.

Importable two ways on purpose:

* `python generator.py` inside a container where the folder *is* the module
  path (that is how `make gen-stream` and the compose `generator` service run);
* `from generator import TransactionSimulator` in tests, scripts and tooling.

`simulator.py` holds the model (who shops where, how much, when);
`profiles.py` swaps the fake catalogue for the real dimensions Postgres holds;
`generator.py` is the CLI that puts the events on Kafka.
"""
from __future__ import annotations

try:                                  # package import (tests / IDE)
    from .profiles import load_profiles, pick_subset
    from .simulator import (
        CardState,
        TransactionSimulator,
        build_cards,
        build_merchants,
        generate_transactions,
        risk_probability,
    )
except ImportError:                     # flat layout inside the container
    from profiles import load_profiles, pick_subset  # type: ignore
    from simulator import (  # type: ignore
        CardState,
        TransactionSimulator,
        build_cards,
        build_merchants,
        generate_transactions,
        risk_probability,
    )

__all__ = [
    "CardState",
    "TransactionSimulator",
    "build_cards",
    "build_merchants",
    "generate_transactions",
    "load_profiles",
    "pick_subset",
    "risk_probability",
]
