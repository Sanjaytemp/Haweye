-- =============================================================================
-- What Postgres needs so Debezium can read the write-ahead log.
--
--   wal_level=logical          -> the WAL keeps enough info to reconstruct rows
--   REPLICA IDENTITY FULL      -> UPDATE/DELETE events carry the OLD row image
--                                 (needed to delete the right Iceberg row)
--   PUBLICATION                -> which tables Postgres publishes to logical
--                                 consumers; Debezium reads exactly this
--   a dedicated role with REPLICATION privilege -> the connector's login
--
-- The replication SLOT is created by Debezium itself (it must, because a slot
-- is a WAL-position cursor and belongs to the consumer).  We do NOT create it
-- here; deleting it manually is one of the two "why is my Postgres disk
-- growing" incidents - see docs/05-cdc.md#operating-safely.
-- =============================================================================

-- the dimension tables live in `public`; they are created by the repo's DDL
CREATE TABLE IF NOT EXISTS public.merchants (
  merchant_id         text PRIMARY KEY,
  merchant_name       text        NOT NULL,
  merchant_category   text        NOT NULL,
  merchant_country    text        NOT NULL,
  merchant_risk_score double precision NOT NULL DEFAULT 0.5,
  merchant_avg_ticket double precision NOT NULL DEFAULT 25,
  merchant_first_seen date        NOT NULL DEFAULT current_date,
  merchant_closed     boolean     NOT NULL DEFAULT false,
  updated_at          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.card_accounts (
  card_id         text PRIMARY KEY,
  customer_id     text        NOT NULL,
  issuer_country  text        NOT NULL,
  credit_limit    double precision NOT NULL DEFAULT 2000,
  txn_limit_1h    double precision NOT NULL DEFAULT 1500,
  travel_notice   boolean     NOT NULL DEFAULT false,
  card_age_days   integer     NOT NULL DEFAULT 365,
  customer_segment text       NOT NULL DEFAULT 'consumer',
  card_status     text        NOT NULL DEFAULT 'active',
  updated_at      timestamptz NOT NULL DEFAULT now()
);

-- keeps updated_at fresh: the CDC merge uses it as the "which version wins" clock
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

ALTER TABLE public.merchants    REPLICA IDENTITY FULL;
ALTER TABLE public.card_accounts REPLICA IDENTITY FULL;

-- Debezium's login (this DB only; never reuse in production)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium') THEN
    CREATE ROLE debezium LOGIN PASSWORD 'debezium' REPLICATION;
  END IF;
END
$$;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

-- one publication for all captured tables (idempotent)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'haweye_cdc') THEN
    CREATE PUBLICATION haweye_cdc FOR TABLE public.merchants, public.card_accounts;
  END IF;
END
$$;
