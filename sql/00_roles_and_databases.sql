-- =============================================================================
-- Runs ONCE, on first boot of an empty postgres volume (docker-entrypoint-initdb.d).
-- Creates the extra databases + the app role the lakehouse needs.
--
--   catalog     -> Iceberg table metadata (JdbcCatalog: "where is table X?")
--   lakehouse   -> operational dimensions (CDC source) + serving tables
--   airflow     -> Airflow's own metadata
--   dimensions  -> CDC demo database (created again by docker-compose.cdc.yml)
-- =============================================================================

-- the role the serving API and the Spark jobs connect as
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'lakehouse_app') THEN
    CREATE ROLE lakehouse_app LOGIN PASSWORD 'lakehouse_app';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airflow') THEN
    CREATE ROLE airflow LOGIN PASSWORD 'airflow' CREATEDB;
  END IF;
END
$$;

SELECT 'CREATE DATABASE catalog OWNER haweye'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'catalog')\gexec

SELECT 'CREATE DATABASE airflow OWNER airflow'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'airflow')\gexec

SELECT 'CREATE DATABASE dimensions OWNER haweye'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dimensions')\gexec

-- the Iceberg catalog database: one schema, owned by the app role too, so the
-- JdbcCatalog can create its `iceberg_namespace_properties` tables itself
\c catalog
CREATE SCHEMA IF NOT EXISTS iceberg_catalog;
ALTER SCHEMA iceberg_catalog OWNER TO haweye;
GRANT ALL ON SCHEMA iceberg_catalog TO haweye;
GRANT ALL ON SCHEMA iceberg_catalog TO lakehouse_app;
GRANT ALL ON SCHEMA iceberg_catalog TO airflow;

\c airflow
GRANT ALL ON SCHEMA public TO airflow;

\c lakehouse
CREATE SCHEMA IF NOT EXISTS serving;
CREATE SCHEMA IF NOT EXISTS staging;
GRANT ALL ON SCHEMA public, serving, staging TO lakehouse_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO lakehouse_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA serving GRANT ALL ON TABLES TO lakehouse_app;
