# Databricks notebook source
# DBTITLE 1,Regular incremental loading (upsert) via Python
import dlt
from pyspark.sql.functions import current_timestamp, expr

# Bronze Layer - could do append or overwrite, we'll append to retain data
@dlt.expect_or_drop("no_null_id", "id IS NOT NULL")
@dlt.table(comment="Raw example csv files experiencing upserts")
def bronze_regular(): # this is the streaming table name
      return ( # normal autoloader code
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .load("/Volumes/bdunm_catalog/ldp_demo/source_data/normal/")
            .selectExpr("*", "_metadata.file_name as source_file_name")
            .withColumn("load_timestamp", current_timestamp())
      )

# Silver Layer - simple Type 1 dimension applying normal upsert
dlt.create_streaming_table(
      name="bdunm_catalog.ldp_demo.silver_regular", 
      comment="Silver example to create type 1 dim just normal upsert",
      expect_all={
        "create_timestamp_date_valid": "date(create_timestamp) <= current_date()",
        "attribute2_starts_with_attr2": "attribute2 LIKE 'attr2%'"
      }
      )

dlt.create_auto_cdc_flow(
  target = "bdunm_catalog.ldp_demo.silver_regular",
  source = "bronze_regular",
  keys = ["id"],
  sequence_by = "create_timestamp",
  except_column_list = ["_rescued_data"],
  stored_as_scd_type = "1"
)

# https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options