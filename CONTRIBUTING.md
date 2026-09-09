# Contributing

Thanks for taking the interest. This project is a teaching codebase *and* a working
pipeline, which means two things for a change: it must run, and a newcomer must be
able to read it. Both are checkable below.

## The loop

```bash
git switch -c add-my-thing
make bootstrap                  # once per clone (creates .venv, builds images)
# edit code
make lint && make test          # the same gates CI runs — before you push
make up                         # only if the change touches the running system
./jobs/submit/run_job.sh feature_store --once --starting earliest   # one job, foreground
./scripts/make_pr.sh            # commit, push, open the PR (or open it by hand)
```

`make test` runs in about a second and needs neither Docker nor a JVM. If a change
cannot be covered by such a test, that's information: it usually means a module
boundary is wrong.

## Layout rules

* `jobs/*.py` — one file per job, readable top to bottom, no logic duplicated
  between jobs. Business logic lives in `jobs/common/`.
* `jobs/common/*.py` — pure definitions (schema, features, rules, cdc) plus the
  two adapters (sparkutils, io). Anything that needs a JVM goes in
  `sparkutils`/`io`, not in a definition module: the unit tests import the
  definitions directly.
* `api/serve.py` — deliberately **self-contained**: no `common.*` import (the API
  image has no pyspark). That is why the rule table is mirrored there, and why
  `tests/unit/test_api_contract.py` compares the two engines on 3,000 random rows.
  If you change `jobs/common/rules.py`, change the mirror in the same commit.
* `docker-compose*.yml` — versions come from `.env.example`; never inline a tag.
* `docs/NN-*.md` — a behaviour change without a doc line is an incomplete change.

## Style, and the reasons behind it

* Comments explain **why**, never what. "we use the history table because a 24h
  stream window keeps state for 24h" is a comment; "increments the counter" is noise.
* Each module starts with a docstring saying what problem it exists to solve. Keep
  it accurate; people read it instead of the code.
* No `except: pass`. If you swallow an error, say out loud (in the message) what
  the degraded behaviour now is — see `model.score_with_model`.
* Fail at the boundary, degrade in the middle, never corrupt on the way out. That's
  the pattern in every job: parse → quality gate → quarantine → idempotent MERGE.
* SQL is generated as strings from `jobs/common/features.py` and friends so the
  batch and streaming paths share one definition. Keep that property: if you add a
  pandas-only or Spark-only copy of a feature, `feature_store.py --verify` will
  report a nonzero `mean_abs_diff` and a reviewer will ask about it.
* `ruff` is the arbiter of formatting arguments (`make lint`); line length 110.

## Tests

| add a… | also add |
|---|---|
| rule | a parametrised case in `tests/unit/test_rules.py` (fires / doesn't fire / boundary value) **and** update the API mirror |
| feature | names in `features.py` + the parity assertion in `tests/unit/test_features_definitions.py` |
| SQL string | an entry in `tests/unit/test_generated_sql.py` (sqlglot) **and** a runnable case in `tests/integration/test_pyspark_sql.py` |
| compose service or env var | `.env.example` entry + a doc line; `make lint` checks the YAML, mounts and interpolation |
| CDC-managed column | `jobs/common/dimensions.py` (one spec → Postgres + Iceberg DDL) — the consistency test does the rest |

`tests/unit` must stay JVM-free: no `pyspark.sql.functions` call at *import or
build* time in a unit test (`F.expr("interval …")` needs a live context — that's
why the quality-gate test reads the source instead of calling it).

## Commit messages

`scope: imperative summary` then why, in the body.

```
features: read 24h aggregates before writing the batch

The prior-window values were wrong for rows in the same micro-batch because
the history already contained them. Swapping the read/write order fixes it and
keeps a replayed batch producing identical numbers (each write is a MERGE).
```

One logical change per commit. `git log --oneline -15` should read like a table of
contents of your PR.

## Pull requests

* `./scripts/make_pr.sh` runs the gates for you and refuses credential-looking or
  >900 KB files.
* Include: what changed, how to run it, and — if behaviour changed — the doc
  section you updated. Screenshots help only for the Airflow/NiFi UI.
* CI has three jobs: `quality` (make lint/test), `spark` (same tests on a JVM),
  `docs` (relative links + Makefile/docs consistency). All three must be green; a
  red `docs` job is almost always a file you renamed without updating a citation.
* The nightly `stack` workflow brings up Docker and pushes a transaction through
  every stage. If it breaks and your PR touched a job, that's yours to fix.
* Branch protection on `main` requires a review and green CI. Squash-merge.

## Data, licences, and what not to commit

Nothing generated goes in: `artifacts/`, `.spark-jars/`, `.run/`, `data/`,
`__pycache__/` are ignored. Never commit a real `.env`, a keytab, a token or
production data — history is public the moment you push. If it happens: revoke the
credential first (assume a bot has it), then tell a maintainer before rewriting
history.

The synthetic dataset is generated, deterministic and seeded (`GEN_SEED`), so
there is no data licence to track. `LICENSE` is MIT; contributions are offered
under it.

## Getting unblocked

Start at `docs/06-runbook.md`; if your symptom isn't there, the fix is to add the
entry — that file is written from real debugging, and it should keep growing.
