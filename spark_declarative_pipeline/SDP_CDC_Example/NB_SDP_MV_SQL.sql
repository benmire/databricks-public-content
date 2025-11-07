-- https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view
-- Create the same MV using SQL - no need to define upsert logic, the Enzyme engine determines it
-- Optimization: https://www.databricks.com/blog/optimizing-materialized-views-recomputes

CREATE OR REFRESH MATERIALIZED VIEW bdunm_catalog.ldp_demo.gold_mv_sql
COMMENT 'Materialized view example using SQL'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.enableDeletionVectors' = 'true'
)
AS
SELECT
  df1.id,
  MIN(df1.attribute1) AS min_attr1,
  MAX(df2.attribute2) AS max_attr2,
  CAST(COUNT(df1.create_timestamp) AS INT) AS count_timestamp
FROM
  bdunm_catalog.ldp_demo.silver_cdc df1
INNER JOIN
  bdunm_catalog.ldp_demo.silver_regular df2
ON
  df1.id = df2.id
GROUP BY
  df1.id;