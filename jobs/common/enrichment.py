"""Enrichment = join the raw feed with the *dimension* tables held in Iceberg.

Dimensions live in Iceberg, so every join uses a consistent, time-travel-able
snapshot of the reference data — that is one concrete benefit of putting
dimensions in a lakehouse table instead of a loose CSV on S3 (docs/03-iceberg.md).
"""
from __future__ import annotations

from pyspark.sql import DataFrame

from . import config

# Latest row per key, in case a dimension table was appended naively instead of
# merged.  Keeps the join strictly 1:1 so a row can never be duplicated.
_DEDIMENT_SQL = """
SELECT * FROM (
  SELECT s.*,
         row_number() OVER (PARTITION BY {key} ORDER BY {ts} DESC NULLS LAST) AS __rn
  FROM {table} s
) WHERE __rn = 1
"""


def latest_rows(table_fqn: str, key: str, ts: str = "source_ts") -> str:
    """SQL fragment: deduplicated dimension view (safe for 1:1 joins)."""
    return _DEDIMENT_SQL.format(table=table_fqn, key=key, ts=ts)


def enrichment_sql(
    batch_view: str = "current_batch",
    *,
    bounds_view: str | None = "bounds",
    merchant_view: str | None = None,
    card_view: str | None = None,
    category_state_view: str | None = None,
) -> str:
    """SQL that turns raw transactions into enriched ones.

    ``merchant_view`` / ``card_view`` / ``category_state_view`` let a caller pass
    *pre-computed broadcast views* (the streaming fast path); when omitted the
    query reads the dimension tables through Iceberg (the batch/backfill path).
    ``bounds_view=None`` drops the partition-pruning filter (tiny tables).
    """
    """Raw transactions + merchant/card dimensions + bounded 24h look-back.

    The micro-batch is exposed as the temp view ``current_batch``; the *history*
    comes straight from the Iceberg table.  Reading history from the table (not
    from Spark state) makes the job stateless, restartable and replayable.
    """
    bounds_cte = """bounds AS (               -- event-time range of this micro-batch -> partition pruning
  SELECT min(event_ts_ts)          AS batch_min_ts,
         max(event_ts_ts)          AS batch_max_ts,
         to_date(min(event_ts_ts)) AS batch_min_dt,
         to_date(max(event_ts_ts)) AS batch_max_dt
  FROM raw
),""" if bounds_view else ""
    merch_cte = (
        f"merch AS (SELECT * FROM {merchant_view}),"
        if merchant_view
        else f"""merch AS (
  SELECT merchant_id, merchant_name, merchant_category, merchant_country,
         merchant_risk_score, merchant_avg_ticket, merchant_first_seen, merchant_closed
  FROM {latest_rows(config.TABLE_MERCHANT_DIM, 'merchant_id')}
),"""
    )
    card_cte = (
        f"card AS (SELECT * FROM {card_view}),"
        if card_view
        else f"""card AS (
  SELECT card_id, customer_id, issuer_country, credit_limit, txn_limit_1h,
         travel_notice, card_age_days, customer_segment, card_status
  FROM {latest_rows(config.TABLE_CARD_DIM, 'card_id')}
),"""
    )
    prior_cte = (
        f"prior AS (SELECT * FROM {category_state_view}),"
        if category_state_view
        else f"""prior AS (                -- (card, merchant_category) pairs already seen
  SELECT e.card_id, e.merchant_category, min(e.event_ts_ts) AS first_seen_ts
  FROM {config.TABLE_ENRICHED} e
  CROSS JOIN bounds b
  WHERE e.dt BETWEEN date_sub(b.batch_min_dt, 3) AND b.batch_max_dt
  GROUP BY e.card_id, e.merchant_category
),"""
    )
    history_cte = (
        f"""history_24h AS (          -- already-persisted transactions for that card, last 24h
  SELECT e.card_id,
         count(*)                      AS txn_count_24h,
         coalesce(sum(e.amount), 0)    AS amount_sum_24h
  FROM {config.TABLE_ENRICHED} e
  CROSS JOIN bounds b
  WHERE e.dt BETWEEN date_sub(b.batch_min_dt, 1) AND b.batch_max_dt
    AND e.event_ts_ts < b.batch_min_ts
  GROUP BY e.card_id
)"""
        if bounds_view
        else ""
    )
    prior_cols = (
        """COALESCE(h.txn_count_24h, 0)   AS txn_count_24h_prior,
       COALESCE(h.amount_sum_24h, 0)  AS amount_sum_24h_prior,"""
        if bounds_view
        else """CAST(NULL AS int)              AS txn_count_24h_prior,
       CAST(NULL AS double)           AS amount_sum_24h_prior,"""
    )
    history_join = "LEFT JOIN history_24h h ON h.card_id = j.card_id" if bounds_view else ""
    history_comma = "," if bounds_view else ""
    _ = history_comma

    return f"""
WITH raw AS (
  SELECT * FROM {batch_view}
),
{bounds_cte}{merch_cte}
{card_cte}
{prior_cte}
joined AS (
  SELECT r.*,
         m.merchant_name,
         COALESCE(m.merchant_category, r.merchant_category)  AS merchant_category,
         COALESCE(m.merchant_country, r.merchant_country)   AS merchant_country,
         m.merchant_risk_score,
         m.merchant_avg_ticket,
         m.merchant_closed,
         c.customer_id,
         c.issuer_country,
         c.credit_limit,
         c.txn_limit_1h,
         c.travel_notice,
         c.card_age_days,
         c.customer_segment,
         c.card_status,
         (c.issuer_country IS NOT NULL
            AND COALESCE(m.merchant_country, r.merchant_country) <> c.issuer_country) AS country_mismatch,
         p.first_seen_ts,
         (p.first_seen_ts IS NULL)                          AS is_new_merchant_category,
         CASE WHEN COALESCE(m.merchant_avg_ticket, 0) > 0
              THEN r.amount / m.merchant_avg_ticket END     AS ratio_to_merchant_avg,
         CASE WHEN COALESCE(c.txn_limit_1h, 0) > 0
              THEN r.amount / c.txn_limit_1h END            AS ratio_to_hourly_limit
  FROM raw r
  LEFT JOIN merch m ON m.merchant_id = r.merchant_id
  LEFT JOIN card  c ON c.card_id = r.card_id
  LEFT JOIN prior p ON p.card_id = r.card_id
                   AND p.merchant_category = COALESCE(m.merchant_category, r.merchant_category)
){history_comma}
{history_cte}
SELECT j.*,
       {prior_cols}
       (j.merchant_closed = TRUE)     AS merchant_closed_hit
FROM joined j
{history_join}
"""


def enrich_batch(spark: DataFrame, current: DataFrame) -> DataFrame:  # noqa: F821 (spark session)
    """Register the micro-batch as a view and run :func:`enrichment_sql`."""
    current.createOrReplaceTempView("current_batch")
    return spark.sql(enrichment_sql("current_batch"))


def enriched_columns() -> list[str]:
    """Stable column order for ``raw.transactions_enriched``."""
    from .schema import TYPED_FIELDS

    return TYPED_FIELDS + [
        "event_ts_ts", "dedup_key", "ingest_ts", "dt",
        "merchant_name", "merchant_risk_score", "merchant_avg_ticket", "merchant_closed",
        "customer_id", "issuer_country", "credit_limit", "txn_limit_1h", "travel_notice",
        "card_age_days", "customer_segment", "card_status",
        "country_mismatch", "first_seen_ts", "is_new_merchant_category",
        "ratio_to_merchant_avg", "ratio_to_hourly_limit",
        "txn_count_24h_prior", "amount_sum_24h_prior", "merchant_closed_hit",
    ]
