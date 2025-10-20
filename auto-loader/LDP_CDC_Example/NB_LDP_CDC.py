# Databricks notebook source
import dlt
from pyspark.sql.functions import current_timestamp, expr

# Bronze Layer - could do append or overwrite, we'll append to retain data
@dlt.table(comment="Raw example csv files with CDC operator")
def bronze_cdc(): # this is the streaming table name
      return ( # normal autoloader code
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .load("/Volumes/bdunm_catalog/ldp_demo/source_data/cdc/")
            .selectExpr("*", "_metadata.file_name as source_file_name")
            .withColumn("load_timestamp", current_timestamp())
      )

# Silver Layer - simple Type 1 dimension applying CDC operators
dlt.create_streaming_table(name="bdunm_catalog.ldp_demo.silver_cdc", comment="Silver CDC example to create type 1 dim")

dlt.create_auto_cdc_flow(
  target = "bdunm_catalog.ldp_demo.silver_cdc",
  source = "bronze_cdc",
  keys = ["id"],
  sequence_by = "create_timestamp",
  apply_as_deletes = expr("cdc_operation = 'DELETE'"),
  except_column_list = ["cdc_operation", "_rescued_data"],
  stored_as_scd_type = "1"
)

# https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options