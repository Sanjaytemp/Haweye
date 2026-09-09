"""The *one and only* definition of the rolling transaction features.

Both paths call into this module, which is what makes a feature store worth
building in the first place: a feature can never be computed one way for
training and another way for serving.

    5min / 1h / 24h        -> count, sum, max, avg, stddev of amount, distinct merchants
    online_1h              -> same, restricted to channel = 'online'
    intl_1h                -> same, restricted to country_mismatch = TRUE

=============================================================================
How the two paths differ (and why the numbers still match)
=============================================================================
`rolling_features_sql` (EXACT, used by the batch backfill and by the tests)
    Window frames over one DataFrame that contains history + current rows:
    ``RANGE BETWEEN <secs> PRECEDING AND CURRENT ROW`` ordered by epoch
    seconds.  Perfectly accurate; needs the history in the frame.

`micro_batch_aggs_sql` + `minute_agg_sql` / `day_agg_sql` (CHEAP, streaming)
    A 10-second micro-batch cannot see the last hour by itself, so the
    streaming job derives those windows from two tiny rolling-aggregate tables
    in the lakehouse (`raw.card_minute_agg`, `raw.card_day_agg`) that it
    MERGEs in the *same* micro-batch.  Subtracting the batch's own contribution
    ("current + prior = history-after-write") keeps the maths exact for
    sum/count and monotone-exact for max.

Why not keep windows in Spark state (`mapGroupsWithState`)?  Because then the
numbers live only inside the job: you cannot audit them, replay them, or train
on them.  Deriving them from the lakehouse keeps a single source of truth.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# (suffix, seconds, extra filter) - one pass per window; the count is small.
ROLLING_WINDOWS: tuple[tuple[str, int, str | None], ...] = (
    ("5min", 5 * 60, None),
    ("1h", 60 * 60, None),
    ("24h", 24 * 60 * 60, None),
    ("online_1h", 60 * 60, "channel = 'online'"),
    ("intl_1h", 60 * 60, "country_mismatch = TRUE"),
)

# metric -> SQL template, used for both the unfiltered windows (`AGGREGATES`, where
# {p} qualifies the column) and the predicate-restricted ones (`FILTERED_AGGREGATES`).
#: ``{p}`` qualifies the column (a join alias, when the same template is reused for
#: a MERGE source); ``{w}`` is where the window spec goes.
#:
#: The window is injected *inside* the expression, directly after the aggregate call,
#: and that is load-bearing: writing ``coalesce(sum(x), 0) OVER (...)`` applies the
#: window to ``coalesce``, leaving a bare ``sum(x)`` behind, and Spark answers
#: ``[MISSING_GROUP_BY]``. Building these strings by appending the window spec at the
#: call site is how that bug arrives, so the templates own the position.
#:
#: ``distinct_merchants`` is ``size(collect_set(...))`` rather than
#: ``count(DISTINCT ...)`` for the same reason in miniature: Spark rejects DISTINCT
#: inside a windowed aggregate, and ``collect_set`` ignores NULLs, which is exactly
#: what the filtered variant needs.
AGGREGATES: dict[str, str] = {
    "txn_count": "count({p}1){w}",
    "amount_sum": "coalesce(sum({p}amount){w}, 0)",
    "amount_max": "coalesce(max({p}amount){w}, 0)",
    "amount_avg": "coalesce(avg({p}amount){w}, 0)",
    "amount_std": "coalesce(stddev_samp({p}amount){w}, 0)",
    "distinct_merchants": "coalesce(size(collect_set({p}merchant_id){w}), 0)",
}

#: rolling-aggregate tables maintained by the streaming feature job
DAY_TABLES = {
    "minute": "raw.card_minute_agg",
    "day": "raw.card_day_agg",
}

MINUTE_AGG_SCHEMA = StructType([
    StructField("card_id", StringType(), False),
    StructField("bucket_ts", TimestampType(), False),
    StructField("dt", DateType(), True),
    StructField("metric_set", StringType(), False),
    StructField("cnt", IntegerType(), True),
    StructField("amt_sum", DoubleType(), True),
    StructField("amt_max", DoubleType(), True),
    StructField("amt_sq", DoubleType(), True),
    StructField("amt_avg", DoubleType(), True),
    StructField("distinct_merchants", IntegerType(), True),
    StructField("merchants", ArrayType(StringType()), True),
    StructField("min_ts", TimestampType(), True),
    StructField("max_ts", TimestampType(), True),
])

DAY_AGG_SCHEMA = StructType([
    StructField("card_id", StringType(), False),
    StructField("dt", DateType(), False),
    StructField("txn_count", IntegerType(), True),
    StructField("amount_sum", DoubleType(), True),
    StructField("amount_max", DoubleType(), True),
    StructField("amount_sq", DoubleType(), True),
    StructField("distinct_merchants", IntegerType(), True),
    StructField("merchants", ArrayType(StringType()), True),
    StructField("categories", ArrayType(StringType()), True),
    StructField("first_seen", TimestampType(), True),
    StructField("last_seen", TimestampType(), True),
])


#: same aggregates, restricted to the rows matching a predicate; every metric the
#: unfiltered table has must appear here, because a filtered window is generated for
#: every metric and a missing key would be a crash in the feature job.
FILTERED_AGGREGATES = {
    "txn_count": "count(CASE WHEN {f} THEN 1 END){w}",
    "amount_sum": "coalesce(sum(CASE WHEN {f} THEN amount END){w}, 0)",
    "amount_max": "coalesce(max(CASE WHEN {f} THEN amount END){w}, 0)",
    "amount_avg": "coalesce(avg(CASE WHEN {f} THEN amount END){w}, 0)",
    "amount_std": "coalesce(stddev_samp(CASE WHEN {f} THEN amount END){w}, 0)",
    "distinct_merchants": "coalesce(size(collect_set(CASE WHEN {f} THEN merchant_id END){w}), 0)",
}


def _agg_expr(metric: str, window_filter: str | None, window: str = "") -> str:
    """One aggregate expression, with the window spec in the only valid position.

    ``window`` is the whole ``OVER (...)`` clause (or "" in a plain GROUP BY), and it
    arrives here instead of being appended by the caller so that a ``coalesce``
    wrapper cannot swallow it.
    """
    if window_filter:
        try:
            return FILTERED_AGGREGATES[metric].format(f=window_filter, w=window)
        except KeyError as exc:
            raise ValueError(f"no filtered form for metric {metric}") from exc
    return AGGREGATES[metric].format(p="", w=window)


#: filtered windows get a *prefix* instead of a suffix, so the generated names are
#: the ones the model contract (NUMERIC_FEATURES), the minute-history pivot and the
#: REST API all use.  Deriving every name from this one function is what keeps
#: `online_txn_count_1h` from becoming `txn_count_online_1h` in one place only -
#: a rename bug that would not raise, it would just train on zeros forever.
FILTERED_PREFIX = {"online_1h": "online", "intl_1h": "international"}


def feature_alias(metric: str, suffix: str) -> str:
    if suffix in FILTERED_PREFIX:
        return f"{FILTERED_PREFIX[suffix]}_{metric}_1h"
    return f"{metric}_{suffix}"


def all_rolling_feature_names() -> list[str]:
    return [feature_alias(m, s) for s, _, _ in ROLLING_WINDOWS for m in AGGREGATES]


def derived_feature_names() -> list[str]:
    return [
        "log_amount", "amount_to_limit", "amount_to_avg_1h", "amount_zscore_24h",
        "amount_vs_merchant_avg_ticket", "hour_of_day", "day_of_week", "is_night", "is_weekend",
    ]


# --------------------------------------------------------------- model inputs
NUMERIC_FEATURES: tuple[str, ...] = (
    "amount", "log_amount", "amount_to_limit", "amount_to_avg_1h", "amount_zscore_24h",
    "ratio_to_merchant_avg", "ratio_to_hourly_limit",
    "txn_count_5min", "txn_count_1h", "amount_sum_5min", "amount_sum_1h", "amount_max_1h",
    "txn_count_24h", "amount_sum_24h", "amount_avg_24h", "amount_std_24h",
    "distinct_merchants_1h", "online_txn_count_1h", "international_txn_count_1h",
    "hour_of_day", "day_of_week", "card_present", "is_3ds", "country_mismatch",
    "is_new_merchant_category", "travel_notice", "merchant_closed_hit",
    "merchant_risk_score", "merchant_avg_ticket", "card_age_days", "credit_limit", "txn_limit_1h",
)
CATEGORICAL_FEATURES: tuple[str, ...] = ("channel", "customer_segment", "merchant_category")
ID_COLUMNS: tuple[str, ...] = ("transaction_id", "event_ts_ts", "dt", "card_id", "customer_id")


def model_feature_names() -> list[str]:
    """Feature order baked into the trained model (a VectorUDT has no names)."""
    return list(NUMERIC_FEATURES) + [f"{c}_idx" for c in CATEGORICAL_FEATURES]


def model_feature_sql() -> str:
    """Cast every model input to double, NULLs to 0 (see docs/07-ml.md)."""
    return ",\n  ".join(
        f"coalesce(cast({name} AS double), 0) AS {name}" for name in model_feature_names()
    )


# ------------------------------------------------------------- physical schema
def features_table_schema() -> StructType:
    fields: list[StructField] = [
        StructField("transaction_id", StringType(), False),
        StructField("event_ts_ts", TimestampType(), True),
        StructField("dt", DateType(), True),
        StructField("card_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("issuer_country", StringType(), True),
        StructField("merchant_country", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("log_amount", DoubleType(), True),
        StructField("currency", StringType(), True),
    ]
    for suffix, _, _ in ROLLING_WINDOWS:
        for metric in AGGREGATES:
            dtype = IntegerType() if metric in ("txn_count", "distinct_merchants") else DoubleType()
            fields.append(StructField(feature_alias(metric, suffix), dtype, True))
    fields += [
        StructField("amount_to_limit", DoubleType(), True),
        StructField("amount_to_avg_1h", DoubleType(), True),
        StructField("amount_zscore_24h", DoubleType(), True),
        StructField("amount_vs_merchant_avg_ticket", DoubleType(), True),
        StructField("ratio_to_merchant_avg", DoubleType(), True),
        StructField("ratio_to_hourly_limit", DoubleType(), True),
        StructField("hour_of_day", IntegerType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("is_night", BooleanType(), True),
        StructField("is_weekend", BooleanType(), True),
        StructField("card_present", BooleanType(), True),
        StructField("is_3ds", BooleanType(), True),
        StructField("country_mismatch", BooleanType(), True),
        StructField("is_new_merchant_category", BooleanType(), True),
        StructField("first_seen_ts", TimestampType(), True),
        StructField("travel_notice", BooleanType(), True),
        StructField("merchant_closed_hit", BooleanType(), True),
        StructField("merchant_risk_score", DoubleType(), True),
        StructField("merchant_avg_ticket", DoubleType(), True),
        StructField("card_age_days", IntegerType(), True),
        StructField("credit_limit", DoubleType(), True),
        StructField("txn_limit_1h", DoubleType(), True),
        StructField("feature_path", StringType(), True),
        StructField("model_uri", StringType(), True),
        StructField("model_version", StringType(), True),
        StructField("computed_at", TimestampType(), True),
    ]
    return StructType(fields)


FEATURES_TABLE_SCHEMA = features_table_schema()
FEATURE_COLUMN_TYPES = {f.name: f.dataType for f in FEATURES_TABLE_SCHEMA.fields}


# --------------------------------------------------- (1) exact, batch / tests
def rolling_features_sql(events_view: str) -> str:
    """Exact as-of rolling aggregates over a view of enriched transactions.

    The input must contain history **and** the rows you want features for
    (that is what makes the window correct); required columns: ``card_id,
    event_ts_ts, amount, merchant_id, channel, country_mismatch``.
    """
    selects = ["e.*"]
    for suffix, seconds, window_filter in ROLLING_WINDOWS:
        frame = f"RANGE BETWEEN {int(seconds)} PRECEDING AND CURRENT ROW"
        window = f"OVER (PARTITION BY e.card_id ORDER BY e.__ts {frame})"
        for metric in AGGREGATES:
            selects.append(f"{_agg_expr(metric, window_filter, window)} "
                           f"AS {feature_alias(metric, suffix)}")
    body = ",\n  ".join(selects)
    return f"""
WITH e AS (
  SELECT *, unix_timestamp(event_ts_ts) AS __ts FROM {events_view}
)
SELECT
  {body}
FROM e
"""


def derived_features_sql(alias: str = "t") -> str:  # noqa: C901 (long but declarative)
    """Ratios / calendar features, computed once rolling aggregates exist."""
    a = alias
    return f"""
  ln(1 + coalesce({a}.amount, 0)) AS log_amount,
  CASE WHEN coalesce({a}.credit_limit, 0) > 0 THEN {a}.amount / {a}.credit_limit END AS amount_to_limit,
  CASE WHEN coalesce({a}.amount_avg_1h, 0) > 0 THEN {a}.amount / {a}.amount_avg_1h END AS amount_to_avg_1h,
  CASE WHEN coalesce({a}.amount_std_24h, 0) > 0
       THEN ({a}.amount - {a}.amount_avg_24h) / {a}.amount_std_24h END AS amount_zscore_24h,
  CASE WHEN coalesce({a}.merchant_avg_ticket, 0) > 0
       THEN {a}.amount / {a}.merchant_avg_ticket END AS amount_vs_merchant_avg_ticket,
  cast(hour({a}.event_ts_ts) AS int) AS hour_of_day,
  cast((dayofweek({a}.event_ts_ts) + 5) %% 7 AS int) AS day_of_week,
  (hour({a}.event_ts_ts) < 6 OR hour({a}.event_ts_ts) >= 23) AS is_night,
  ((dayofweek({a}.event_ts_ts) + 5) %% 7 >= 5) AS is_weekend
""".replace("%%", "%")


def compute_features(events: DataFrame, view_name: str = "feature_events") -> DataFrame:
    """Exact path: rolling + derived features for one DataFrame of transactions."""
    events.createOrReplaceTempView(view_name)
    spark = events.sparkSession
    rolling = spark.sql(rolling_features_sql(view_name))
    rolling.createOrReplaceTempView(view_name + "__rolling")
    return spark.sql(f"SELECT t.*, {derived_features_sql()} FROM {view_name}__rolling t")


# ------------------------------------------- (2) streaming: batch + aggregates
def micro_batch_aggs_sql(view: str) -> str:
    """Exact per-row windows *within* the micro-batch (5min/1h/online/intl)."""
    selects = ["e.*"]
    for suffix, seconds, window_filter in ROLLING_WINDOWS:
        if seconds > 3600:
            continue  # 24h is derived from the day table instead
        frame = f"RANGE BETWEEN {int(seconds)} PRECEDING AND CURRENT ROW"
        window = f"OVER (PARTITION BY e.card_id ORDER BY e.__ts {frame})"
        for metric in AGGREGATES:
            if metric == "distinct_merchants":
                continue  # needs the rolling tables to be meaningful
            selects.append(f"{_agg_expr(metric, window_filter, window)} "
                           f"AS {feature_alias(metric, suffix)}")
    body = ",\n  ".join(selects)
    return f"""
WITH e AS (
  SELECT *, unix_timestamp(event_ts_ts) AS __ts FROM {view}
)
SELECT {body} FROM e
"""


def minute_agg_sql(batch_view: str) -> str:
    """(card x minute x metric_set) rollup of the current micro-batch."""
    blocks = []
    for metric_set, filt in (("std", None), ("online", "channel = 'online'"),
                             ("intl", "country_mismatch = TRUE")):
        where = f"WHERE {filt}" if filt else ""
        blocks.append(f"""
SELECT card_id,
       date_trunc('MINUTE', event_ts_ts)              AS bucket_ts,
       to_date(event_ts_ts)                           AS dt,
       '{metric_set}'                                 AS metric_set,
       count(1)                                       AS cnt,
       coalesce(sum(amount), 0)                       AS amt_sum,
       coalesce(max(amount), 0)                       AS amt_max,
       coalesce(sum(amount * amount), 0)              AS amt_sq,
       coalesce(avg(amount), 0)                       AS amt_avg,
       coalesce(count(DISTINCT merchant_id), 0)       AS distinct_merchants,
       slice(array_distinct(collect_set(CAST(merchant_id AS string))), 1, 50) AS merchants,
       min(event_ts_ts)                               AS min_ts,
       max(event_ts_ts)                               AS max_ts
FROM {batch_view}
{where}
GROUP BY card_id, date_trunc('MINUTE', event_ts_ts), to_date(event_ts_ts)""")
    return "\nUNION ALL\n".join(blocks)


def day_agg_sql(batch_view: str) -> str:
    """(card x day) rollup of the current micro-batch (feeds the 24h window)."""
    return f"""
SELECT card_id,
       to_date(event_ts_ts)                             AS dt,
       count(1)                                         AS txn_count,
       coalesce(sum(amount), 0)                         AS amount_sum,
       coalesce(max(amount), 0)                         AS amount_max,
       coalesce(sum(amount * amount), 0)                AS amount_sq,
       coalesce(count(DISTINCT merchant_id), 0)         AS distinct_merchants,
       slice(array_distinct(collect_set(CAST(merchant_id AS string))), 1, 50)     AS merchants,
       slice(array_distinct(collect_set(CAST(merchant_category AS string))), 1, 50) AS categories,
       min(event_ts_ts)                                 AS first_seen,
       max(event_ts_ts)                                 AS last_seen
FROM {batch_view}
GROUP BY card_id, to_date(event_ts_ts)
"""


def _minute_window_cte(bounds_view: str, metric_sets: str, mode: str) -> str:
    """Build the "rolling aggregate over the minute table" CTE body.

    `mode="prior"` keeps only buckets that closed *before* the micro-batch
    started, which is what makes "strictly prior" correct for count/sum.
    """
    sets = ", ".join(f"'{m}'" for m in metric_sets.split(","))
    where_mode = f"AND m.bucket_ts < (SELECT start_ts FROM {bounds_view})" if mode == "prior" else ""
    return f"""
  SELECT m.card_id,
         m.metric_set,
         coalesce(sum(m.cnt), 0)      AS cnt,
         coalesce(sum(m.amt_sum), 0)  AS amt_sum,
         coalesce(max(m.amt_max), 0)  AS amt_max,
         coalesce(avg(m.amt_avg), 0)  AS amt_avg,
         coalesce(sum(m.distinct_merchants), 0) AS distinct_merchants
  FROM {DAY_TABLES['minute']} m
  WHERE m.metric_set IN ({sets})
    AND m.dt BETWEEN date_sub((SELECT min_dt FROM {bounds_view}), 1)
                 AND (SELECT max_dt FROM {bounds_view})
    {where_mode}
  GROUP BY m.card_id, m.metric_set
"""


def minute_history_sql(bounds_view: str = "batch_bounds") -> str:
    """Card-level rolling aggregates read from `raw.card_minute_agg`.

    ``bounds_view`` must expose `start_ts`, `min_dt`, `max_dt` (see
    :func:`batch_bounds_sql`).  Because the history is read *before* the
    micro-batch is merged into the rolling tables, "strictly prior" falls out of
    the read order for free (`mode="prior"`).
    """
    return f"""
WITH hist AS ({_minute_window_cte(bounds_view, "std,online,intl", "prior")})
SELECT card_id,
       max(CASE WHEN metric_set = 'std'    THEN cnt END)       AS txn_count_1h,
       max(CASE WHEN metric_set = 'std'    THEN amt_sum END)   AS amount_sum_1h,
       max(CASE WHEN metric_set = 'std'    THEN amt_max END)   AS amount_max_1h,
       max(CASE WHEN metric_set = 'std'    THEN amt_avg END)   AS amount_avg_1h,
       max(CASE WHEN metric_set = 'std'    THEN distinct_merchants END) AS distinct_merchants_1h,
       max(CASE WHEN metric_set = 'online' THEN cnt END)       AS online_txn_count_1h,
       max(CASE WHEN metric_set = 'online' THEN amt_sum END)   AS online_amount_sum_1h,
       max(CASE WHEN metric_set = 'intl'   THEN cnt END)       AS international_txn_count_1h,
       max(CASE WHEN metric_set = 'intl'   THEN amt_sum END)   AS international_amount_sum_1h
FROM hist
GROUP BY card_id
"""


def batch_bounds_sql(batch_view: str) -> str:
    """Helper frame: event-time bounds of the micro-batch (used by the history reads)."""
    return f"""
SELECT min(event_ts_ts)          AS start_ts,
       to_date(min(event_ts_ts)) AS min_dt,
       to_date(max(event_ts_ts)) AS max_dt,
       coalesce(sum(amount), 0)  AS batch_amount_sum,
       coalesce(sum(amount * amount), 0) AS batch_amount_sq,
       count(1)                  AS batch_txn_count
FROM {batch_view}
"""


def day_history_sql(batch_view: str, bounds_view: str | None = None) -> str:
    """24h numbers (prior-only) + newness flags, from `raw.card_day_agg`.

    The day table buckets by *day*, so within the same day the table already
    contains the current batch's rows once it was merged; that is why the sums
    subtract the batch's own contribution (`batch_amount_sum` etc.).  The new
    merchant-category flag needs no subtraction: the read happens before the
    merge, so `categories` only holds what was seen *before* this batch.
    """
    return f"""
WITH bounds AS ({batch_bounds_sql(batch_view)}),
hist AS (
  SELECT d.*
  FROM {DAY_TABLES['day']} d
  WHERE d.dt BETWEEN date_sub((SELECT min_dt FROM bounds), 1) AND (SELECT max_dt FROM bounds)
),
sums AS (
  SELECT h.card_id,
         coalesce(sum(h.txn_count), 0)   AS txn_count,
         coalesce(sum(h.amount_sum), 0)  AS amount_sum,
         coalesce(sum(h.amount_sq), 0)   AS amount_sq,
         coalesce(max(h.amount_max), 0)  AS amount_max,
         coalesce(min(h.first_seen), current_timestamp()) AS first_seen
  FROM hist h GROUP BY h.card_id
),
cats AS (
  SELECT h.card_id, array_distinct(flatten(collect_list(h.categories))) AS categories
  FROM hist h GROUP BY h.card_id
)
SELECT s.card_id,
       greatest(s.txn_count - b.batch_txn_count, 0)  AS txn_count_24h,
       greatest(s.amount_sum - b.batch_amount_sum, 0) AS amount_sum_24h,
       s.amount_max                                    AS amount_max_24h,
       CASE WHEN s.txn_count - b.batch_txn_count > 0
            THEN greatest(s.amount_sum - b.batch_amount_sum, 0)
                 / (s.txn_count - b.batch_txn_count) END AS amount_avg_24h,
       CASE WHEN s.txn_count - b.batch_txn_count > 1
            THEN sqrt(greatest(
                   (greatest(s.amount_sq - b.batch_amount_sq, 0)) / (s.txn_count - b.batch_txn_count)
                   - pow(greatest(s.amount_sum - b.batch_amount_sum, 0)
                         / (s.txn_count - b.batch_txn_count), 2), 0)) END AS amount_std_24h,
       s.first_seen                                    AS card_first_seen,
       (s.txn_count - b.batch_txn_count) = 0           AS is_new_card,
       coalesce(c.categories, array())                 AS prior_categories
FROM sums s CROSS JOIN bounds b
LEFT JOIN cats c ON c.card_id = s.card_id
"""


def to_feature_store_layout(df: DataFrame, model_uri: str = "none", model_version: str = "none") -> DataFrame:
    """Project a feature frame into the physical feature-store schema (by name)."""
    have = set(df.columns)
    out = df
    for field in FEATURES_TABLE_SCHEMA.fields:
        name = field.name
        if name == "model_uri":
            out = out.withColumn(name, F.lit(model_uri))
        elif name == "model_version":
            out = out.withColumn(name, F.lit(model_version))
        elif name == "computed_at":
            out = out.withColumn(name, F.current_timestamp())
        elif name not in have:
            out = out.withColumn(name, F.lit(None).cast(field.dataType))
    out = out.select(*[f.name for f in FEATURES_TABLE_SCHEMA.fields])
    return out.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in FEATURES_TABLE_SCHEMA.fields])


def parse_feature_json(df: DataFrame) -> DataFrame:
    """Decode the compact feature event the feature job publishes to Kafka.

    Same schema object as the offline table -> downstream consumers inherit any
    schema change automatically (and break loudly if a type changed, which is
    what you want, not silent NULLs).
    """
    return df.withColumn("feats", F.from_json(F.col("value").cast("string"), FEATURES_TABLE_SCHEMA)).select(
        *[F.col(f"feats.{f.name}").alias(f.name) for f in FEATURES_TABLE_SCHEMA.fields]
    )


def ensure_feature_columns(df: DataFrame, *, include_label: bool = False) -> DataFrame:
    """Add/normalise the model's inputs on any frame (cast to double, NULL -> 0).

    Used by the streaming scorer, the training job and the REST API fallback:
    one helper guarantees every consumer builds *the same* vector.
    """
    a = F.col
    # (inputs this needs, expression) - the inputs are checked so that the helper is
    # total: a frame that arrives without `event_ts_ts` (a hand-built REST request)
    # gets NULL features instead of an AnalysisException about an unknown column.
    derived: dict[str, tuple[list[str], object]] = {
        "log_amount": (["amount"], F.log(1 + F.coalesce(a("amount"), F.lit(0.0)))),
        "amount_to_limit": (["amount", "credit_limit"],
                            F.when(a("credit_limit") > 0,
                                   a("amount") / F.nullif(a("credit_limit"), F.lit(0.0)))),
        "amount_to_avg_1h": (["amount", "amount_avg_1h"],
                             F.when(a("amount_avg_1h") > 0,
                                    a("amount") / F.nullif(a("amount_avg_1h"), F.lit(0.0)))),
        "amount_zscore_24h": (["amount", "amount_avg_24h", "amount_std_24h"],
                              F.when(a("amount_std_24h") > 0,
                                     (a("amount") - a("amount_avg_24h"))
                                     / F.nullif(a("amount_std_24h"), F.lit(0.0)))),
        "amount_vs_merchant_avg_ticket": (["amount", "merchant_avg_ticket"],
                                          F.when(a("merchant_avg_ticket") > 0,
                                                 a("amount") / F.nullif(a("merchant_avg_ticket"), F.lit(0.0)))),
        "hour_of_day": (["event_ts_ts"], F.hour(a("event_ts_ts"))),
        "day_of_week": (["event_ts_ts"], ((F.dayofweek(a("event_ts_ts")) + 5) % 7).cast("int")),
        "is_night": (["event_ts_ts"], ((F.hour(a("event_ts_ts")) < 6) | (F.hour(a("event_ts_ts")) >= 23))),
        "is_weekend": (["event_ts_ts"], (((F.dayofweek(a("event_ts_ts")) + 5) % 7) >= 5)),
        "amount_std_24h": (["txn_count_24h", "amount_std_24h"],
                           F.when(a("txn_count_24h") > 1, F.coalesce(a("amount_std_24h"), F.lit(None)))),
        "amount_avg_24h": (["txn_count_24h", "amount_sum_24h"],
                           F.when(a("txn_count_24h") > 0, a("amount_sum_24h") / a("txn_count_24h"))),
    }
    out = df
    missing = [name for name in NUMERIC_FEATURES if name not in out.columns]
    # Two passes, and the order is the fix: the derived expressions reference *other
    # features* (`amount_avg_24h` divides by `txn_count_24h`), so one loop would
    # build them in NUMERIC_FEATURES order and throw on whichever dependency had not
    # been added yet -- silently fine in the streaming job (the feature table has
    # every column) and a crash for any other consumer, which is the whole reason
    # this helper exists.  Placeholder first, derive second.
    for name in missing:
        out = out.withColumn(name, F.lit(None).cast("double"))
    have = set(out.columns)
    for name in missing:
        spec = derived.get(name) or _derived_from_window(name)
        if spec is None:
            continue  # nothing to derive it from: the NULL placeholder stands
        needs, expr = spec
        if all(col in have for col in needs):
            out = out.withColumn(name, expr)
    cols = [F.col(k) for k in ID_COLUMNS if k in out.columns]
    if "dt" in out.columns:
        cols.append(F.col("dt"))
    if include_label and "label" in out.columns:
        cols.append(F.col("label").cast("double"))
    for name in NUMERIC_FEATURES:
        cols.append(F.coalesce(F.col(name).cast("double"), F.lit(0.0)).alias(name))
    for c in CATEGORICAL_FEATURES:
        cols.append(F.coalesce(F.col(c), F.lit("unknown")).alias(c))
    return out.select(*cols)


#: ratios that are not in `derived` because they read dimension columns rather
#: than other features: name -> (inputs, numerator, denominator)
WINDOW_FREE_RATIOS = {
    "ratio_to_merchant_avg": ("amount", "merchant_avg_ticket"),
    "ratio_to_hourly_limit": ("amount", "txn_limit_1h"),
}


def _derived_from_window(name: str):
    """Ratios that read dimension columns instead of other features.

    Returns ``(inputs, expression)`` -- the inputs are what
    `ensure_feature_columns` checks for, so a frame without `txn_limit_1h` yields a
    NULL feature rather than an unresolved-column error.
    """
    pair = WINDOW_FREE_RATIOS.get(name)
    if pair is None:
        return None
    num, den = pair
    return [num, den], F.when(F.col(den) > 0, F.col(num) / F.nullif(F.col(den), F.lit(0.0)))


# --------------------------------------------------------------- pandas helper
def rolling_features_pandas(events):
    """Reference implementation used by `tests/unit/test_features_parity.py`.

    Deliberately dumb (a nested loop) so it can be trusted as an oracle for the
    SQL above.  Input: pandas frame with card_id, event_ts_ts, amount.
    """
    import pandas as pd

    df = events.copy()
    df["event_ts_ts"] = pd.to_datetime(df["event_ts_ts"])
    df = df.sort_values(["card_id", "event_ts_ts"]).reset_index(drop=True)
    for label, seconds in (("5min", 300), ("1h", 3600), ("24h", 86400)):
        count, total, biggest = [], [], []
        for _, row in df.iterrows():
            prior = df[(df["card_id"] == row["card_id"])
                       & (df["event_ts_ts"] <= row["event_ts_ts"])
                       & (df["event_ts_ts"] >= row["event_ts_ts"] - pd.Timedelta(seconds=seconds))]
            count.append(len(prior))
            total.append(float(prior["amount"].sum()))
            biggest.append(float(prior["amount"].max()) if len(prior) else 0.0)
        df[f"txn_count_{label}"] = count
        df[f"amount_sum_{label}"] = total
        df[f"amount_max_{label}"] = biggest
    return df
