-- =============================================================================
-- Nothing to create here: Iceberg's JdbcCatalog creates its two tables
-- (`iceberg_namespace_properties`, `iceberg_tables`) on first connect.
-- This file only grants what the app role needs and documents the layout, so
-- that `psql -d catalog` is not a mystery.
--
--   iceberg_tables                row per table: (catalog, ns, name, metadata_location)
--   iceberg_namespace_properties  row per namespace property
--
-- The *data* is never here: it is parquet + metadata json on MinIO, referenced
-- by `metadata_location`.  That separation is what makes Iceberg "a table format".
-- =============================================================================
\c catalog
GRANT ALL ON ALL TABLES IN SCHEMA iceberg_catalog TO lakehouse_app;
GRANT ALL ON ALL TABLES IN SCHEMA iceberg_catalog TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA iceberg_catalog GRANT ALL ON TABLES TO lakehouse_app;
