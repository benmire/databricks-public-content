---- Example of Upsert Using SQL - No declaration of SCD operator ----

-- Bronze Layer - will just re-use existing bronze as no reason to redo

-- Silver Layer - standard upsert to silver table
CREATE OR REFRESH STREAMING TABLE bdunm_catalog.ldp_demo.silver_regular_sql_no_scd;

CREATE FLOW upsert_flow_sql_no_scd as AUTO CDC INTO
  bdunm_catalog.ldp_demo.silver_regular_sql_no_scd
FROM stream(bdunm_catalog.ldp_demo.bronze_regular_sql)
KEYS
  (id)
SEQUENCE BY
  create_timestamp;

-- https://docs.databricks.com/aws/en/dlt/cdc?language=SQL