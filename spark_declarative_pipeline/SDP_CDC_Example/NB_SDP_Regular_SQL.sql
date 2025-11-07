---- Example of Upsert Using SQL ----

-- Bronze Layer - could do append or overwrite, we'll append to retain data using Autoloader via SQL
CREATE OR REFRESH STREAMING TABLE bdunm_catalog.ldp_demo.bronze_regular_sql
AS
SELECT *, _metadata.file_name as source_file_name, current_timestamp() as load_timestamp
FROM STREAM read_files(
  "/Volumes/bdunm_catalog/ldp_demo/source_data/normal_sql/",
  format => "csv",
  inferColumnTypes => "true"
);

-- Silver Layer - simple Type 1 dimension applying normal upsert created by SQL
-- Upsert (same as MERGE where you declare join keys, how to handle duplicates, etc.)
CREATE OR REFRESH STREAMING TABLE bdunm_catalog.ldp_demo.silver_regular_sql
(
  CONSTRAINT create_timestamp_not_future EXPECT (create_timestamp <= current_timestamp()),
  CONSTRAINT attribute1_starts_with EXPECT (attribute1 LIKE 'attr1%')
);

CREATE FLOW upsert_flow_sql as AUTO CDC INTO
  bdunm_catalog.ldp_demo.silver_regular_sql
FROM stream(bdunm_catalog.ldp_demo.bronze_regular_sql)
KEYS
  (id)
SEQUENCE BY
  create_timestamp
COLUMNS * EXCEPT
  (_rescued_data)
STORED AS
  SCD TYPE 1;

-- Append-only
CREATE OR REFRESH STREAMING TABLE bdunm_catalog.ldp_demo.silver_regular_sql_simple
AS
SELECT *
FROM STREAM(bdunm_catalog.ldp_demo.bronze_regular_sql)
;

-- https://docs.databricks.com/aws/en/dlt/cdc?language=SQL