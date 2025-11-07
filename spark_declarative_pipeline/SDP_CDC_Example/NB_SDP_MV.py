# Materialized views are created in instances of joins, aggregations, etc. where results are pre-computed
# Python - https://docs.databricks.com/aws/en/dlt-ref/dlt-python-ref-table
# SQL - https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view
# Optimization - https://www.databricks.com/blog/optimizing-materialized-views-recomputes

import dlt
from pyspark.sql.functions import min, max, avg, count, col

@dlt.table(
  name = "bdunm_catalog.ldp_demo.gold_mv",
  comment = "Materialized view example using Python",
  table_properties = {
    'delta.autoOptimize.optimizeWrite':'true',
    'delta.autoOptimize.autoCompact':'true',
    'delta.enableRowTracking':'true',
    'delta.enableDeletionVectors':'true'
  },
  schema = """ 
    id INT,
    min_attr1 STRING,
    max_attr2 STRING,
    count_timestamp INTEGER,
    date INTEGER GENERATED ALWAYS AS (count_timestamp + 1)
    """, # this can also be pre-defined using struct_type or let LDP infer the schema
  cluster_by_auto = True,
)
def gold_mv():
  # Define source dataframes
  df1 = dlt.read("bdunm_catalog.ldp_demo.silver_cdc")
  df2 = dlt.read("bdunm_catalog.ldp_demo.silver_regular")

  # Joined DF
  df_joined = df1.join(
      df2,
      on="id",
      how="inner"
  ).select(
      df1["id"],
      df1["attribute1"],
      df2["attribute2"],
      df1["create_timestamp"]
  )

  # Aggregate
  df_agg = df_joined.groupBy("id").agg(
              min("attribute1").alias("min_attr1"),
              max("attribute2").alias("max_attr2"),
              count("create_timestamp").alias("count_timestamp").cast("INT")
  )

  return df_agg