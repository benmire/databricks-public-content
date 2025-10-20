---- Example of CDC Using SQL ----

-- Bronze Layer - could do append or overwrite, we'll append to retain data using Autoloader via SQL
CREATE OR REFRESH STREAMING TABLE bdunm_catalog.ldp_demo.bronze_cdc_sql
AS
SELECT *, _metadata.file_name as source_file_name, current_timestamp() as load_timestamp
FROM STREAM read_files(
  "/Volumes/bdunm_catalog/ldp_demo/source_data/cdc_sql/",
  format => "csv",
  inferColumnTypes => "true"
);

-- Silver Layer - simple Type 1 dimension applying CDC operators created by SQL
CREATE OR REFRESH STREAMING TABLE bdunm_catalog.ldp_demo.silver_cdc_sql;

CREATE FLOW cdc_flow_sql as AUTO CDC INTO
  bdunm_catalog.ldp_demo.silver_cdc_sql
FROM stream(bdunm_catalog.ldp_demo.bronze_cdc_sql)
KEYS
  (id)
APPLY AS DELETE WHEN
  cdc_operation = "DELETE"
SEQUENCE BY
  create_timestamp
COLUMNS * EXCEPT
  (cdc_operation, _rescued_data)
STORED AS
  SCD TYPE 1;

-- https://docs.databricks.com/aws/en/dlt/cdc?language=SQL