-- =============================================================================
-- Operational dimension tables (the "system of record" for reference data).
--
-- These are *application* tables, not analytics tables: the fraud app owns
-- them, they are small, and they are what Debezium captures.  The lakehouse
-- copy (Iceberg `dim.*`) is a *derived* replica, refreshed by CDC.
-- Idempotent: safe to re-run (make seed-dims runs this file, then inserts data).
-- =============================================================================

CREATE TABLE IF NOT EXISTS public.merchants (
  merchant_id         text PRIMARY KEY,
  merchant_name       text        NOT NULL,
  merchant_category   text        NOT NULL,
  merchant_country    text        NOT NULL,
  merchant_risk_score double precision NOT NULL DEFAULT 0.5 CHECK (merchant_risk_score BETWEEN 0 AND 1),
  merchant_avg_ticket double precision NOT NULL DEFAULT 25 CHECK (merchant_avg_ticket > 0),
  merchant_first_seen date        NOT NULL DEFAULT current_date,
  merchant_closed     boolean     NOT NULL DEFAULT false,
  updated_at          timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE public.merchants IS
  'Merchant master. Source of truth for enrichment; CDC-captured into Iceberg.';

CREATE TABLE IF NOT EXISTS public.card_accounts (
  card_id         text PRIMARY KEY,
  customer_id     text        NOT NULL,
  issuer_country  text        NOT NULL,
  credit_limit    double precision NOT NULL DEFAULT 2000 CHECK (credit_limit > 0),
  txn_limit_1h    double precision NOT NULL DEFAULT 1500 CHECK (txn_limit_1h > 0),
  travel_notice   boolean     NOT NULL DEFAULT false,
  card_age_days   integer     NOT NULL DEFAULT 365,
  customer_segment text       NOT NULL DEFAULT 'consumer',
  card_status     text        NOT NULL DEFAULT 'active',
  updated_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS card_accounts_customer_idx ON public.card_accounts (customer_id);

CREATE TABLE IF NOT EXISTS public.transaction_labels (
  transaction_id text PRIMARY KEY,
  label          smallint NOT NULL DEFAULT 0 CHECK (label IN (0, 1)),
  fraud_type     text,
  labelled_at    timestamptz NOT NULL DEFAULT now(),
  source         text NOT NULL DEFAULT 'generator'
);
COMMENT ON TABLE public.transaction_labels IS
  'Ground truth, arrives late (disputes/chargebacks). Kept OUT of the feature '
  'table on purpose: a feature table that stores its own target invites leakage.';

-- touch updated_at so CDC has a monotone version to compare against
CREATE OR REPLACE FUNCTION public.touch_updated_at() RETURNS trigger AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS merchants_touch ON public.merchants;
CREATE TRIGGER merchants_touch BEFORE UPDATE ON public.merchants
  FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at();

DROP TRIGGER IF EXISTS card_accounts_touch ON public.card_accounts;
CREATE TRIGGER card_accounts_touch BEFORE UPDATE ON public.card_accounts
  FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at();
