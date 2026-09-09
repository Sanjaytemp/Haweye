-- =============================================================================
-- Serving / operational store — the OLTP side of the lakehouse.
--
--   fraud_scores            one row per scored transaction (the audit trail)
--   fraud_alerts            the queue a human works through
--   transactions_feature_store  the online copy of the latest feature vector
--   merchants_lakehouse /   mirrors of the Iceberg dimension tables, written by
--   card_accounts_lakehouse     the CDC job so the app can join without Spark
--
-- The Iceberg tables stay the system of record; these exist because an alert
-- console must answer in milliseconds, not in "waiting for a Spark job".
-- =============================================================================

CREATE TABLE IF NOT EXISTS public.fraud_scores (
  transaction_id  text PRIMARY KEY,
  card_id         text,
  event_ts_ts     timestamptz,
  amount          double precision,
  model_version   text,
  model_score     double precision,
  rule_score      double precision,
  final_score     double precision NOT NULL,
  decision        text NOT NULL,
  rule_hits       text[] NOT NULL DEFAULT '{}',
  feature_snapshot jsonb,
  scored_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS fraud_scores_card_time_idx ON public.fraud_scores (card_id, event_ts_ts DESC);
CREATE INDEX IF NOT EXISTS fraud_scores_decision_idx ON public.fraud_scores (decision, scored_at DESC);
CREATE INDEX IF NOT EXISTS fraud_scores_score_idx ON public.fraud_scores (final_score DESC);

CREATE TABLE IF NOT EXISTS public.fraud_alerts (
  alert_id       bigserial PRIMARY KEY,
  transaction_id text NOT NULL,
  card_id        text,
  score          double precision,
  decision       text,
  reasons        text,
  status         text NOT NULL DEFAULT 'open',   -- open | investigating | cleared | confirmed
  assigned_to    text,
  opened_at      timestamptz NOT NULL DEFAULT now(),
  closed_at      timestamptz,
  notes          text,
  CONSTRAINT fraud_alerts_txn_uniq UNIQUE (transaction_id)
);
CREATE INDEX IF NOT EXISTS fraud_alerts_status_idx ON public.fraud_alerts (status, opened_at DESC);

CREATE TABLE IF NOT EXISTS public.transactions_feature_store (
  transaction_id  text PRIMARY KEY,
  card_id         text,
  customer_id     text,
  merchant_id     text,
  event_ts_ts     timestamptz,
  amount          double precision,
  txn_count_5min  integer,
  txn_count_1h    integer,
  amount_sum_5min double precision,
  amount_sum_1h   double precision,
  amount_max_1h   double precision,
  txn_count_24h   integer,
  amount_sum_24h  double precision,
  distinct_merchants_1h integer,
  online_txn_count_1h   integer,
  international_txn_count_1h integer,
  hour_of_day     integer,
  channel         text,
  country_mismatch boolean,
  updated_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS fs_card_time_idx ON public.transactions_feature_store (card_id, event_ts_ts DESC);

-- mirrors written by the CDC job (see jobs/common/cdc_merge_common.py)
CREATE TABLE IF NOT EXISTS public.merchants_lakehouse (
  merchant_id         text PRIMARY KEY,
  merchant_name       text,
  merchant_category   text,
  merchant_country    text,
  merchant_risk_score double precision,
  merchant_avg_ticket double precision,
  merchant_first_seen date,
  merchant_closed     boolean,
  source_ts           timestamptz,
  op                  text,
  synced_at           timestamptz
);
CREATE TABLE IF NOT EXISTS public.card_accounts_lakehouse (
  card_id         text PRIMARY KEY,
  customer_id     text,
  issuer_country  text,
  credit_limit    double precision,
  txn_limit_1h    double precision,
  travel_notice   boolean,
  card_age_days   integer,
  customer_segment text,
  card_status     text,
  source_ts       timestamptz,
  op              text,
  synced_at       timestamptz
);

-- small helper view the alert console queries (documented in docs/06-runbook.md)
CREATE OR REPLACE VIEW public.v_open_alert_detail AS
SELECT a.alert_id, a.transaction_id, a.card_id, a.score, a.reasons, a.opened_at,
       s.decision, s.amount, s.model_score, s.rule_score, s.model_version,
       m.merchant_name, m.merchant_country, m.merchant_risk_score,
       c.customer_segment, c.issuer_country
FROM public.fraud_alerts a
LEFT JOIN public.fraud_scores s ON s.transaction_id = a.transaction_id
LEFT JOIN public.merchants_lakehouse m ON m.merchant_id = s.feature_snapshot ->> 'merchant_id'
LEFT JOIN public.card_accounts_lakehouse c ON c.card_id = s.card_id
WHERE a.status = 'open';
