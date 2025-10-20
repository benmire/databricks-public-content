# Databricks notebook source
dbutils.fs.ls("/Volumes/bdunm_catalog/ldp_demo/source_data/autoloader_simple/")

# COMMAND ----------

# DBTITLE 1,Read source csv (could be any file)
from pyspark.sql.functions import current_timestamp

# Read from Volume (cloudFiles tells this to do Auto Loader)
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.schemaLocation", "/Volumes/bdunm_catalog/ldp_demo/source_data/checkpoint/autoloader_simple")
      .option("cloudFiles.cleanSource", "DELETE")  # DELETE source files after processing (based on retention)
      # can also use MOVE, and move to cloudFiles.cleanSource.moveDestination
      # .option("cloudFiles.allowOverwrites", "true")
      .option("cloudFiles.cleanSource.retentionDuration", "8 days") # Minimum is 7 days for DELETE, no minimum for MOVE - default is 30
      .load("/Volumes/bdunm_catalog/ldp_demo/source_data/autoloader_simple/")
      .selectExpr("*", "_metadata.file_name as source_file_name")
      .withColumn("load_timestamp", current_timestamp()))

# Write to a Delta table
(df.writeStream
   .format("delta")
   .option("checkpointLocation", "/Volumes/bdunm_catalog/ldp_demo/source_data/checkpoint/autoloader_simple") # keep this separate
   .option("mergeSchema", "true") # allows for schema evolution
   .outputMode("append") # can also overwrite each load if you want to clear out landing
   .trigger(availableNow=True) # means it will process new files that haven't been processed and then wait to be run again
   .table("bdunm_catalog.ldp_demo.bronze_autoloader_simple"))

# https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from bdunm_catalog.ldp_demo.bronze_autoloader_simple

# COMMAND ----------

# DBTITLE 1,Create Silver table
# Create silver table if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS bdunm_catalog.ldp_demo.silver_autoloader_simple_scd (
  id INT NOT NULL,
  create_timestamp TIMESTAMP,
  attribute1 STRING,
  attribute2 STRING,
  attribute3 STRING,
  cdc_operation STRING,
  _rescued_data STRING,
  source_file_name STRING,
  load_timestamp TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

# DBTITLE 1,Silver Processing
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Read source table
source_df = spark.table("bdunm_catalog.ldp_demo.bronze_autoloader_simple")

# Get latest record per id by create_timestamp DESC
window_spec = Window.partitionBy("id").orderBy(source_df["create_timestamp"].desc())
latest_df = source_df.withColumn("rn", row_number().over(window_spec)).filter("rn = 1").drop("rn")

# Define target table name
target_table = "bdunm_catalog.ldp_demo.silver_autoloader_simple_scd"
delta_table = DeltaTable.forName(spark, target_table)

# Merge into target table (Type 1 SCD: overwrite with latest)
(
  delta_table.alias("target")
  .merge(
    latest_df.alias("source"),
    "target.id = source.id"
  )
  .whenMatchedUpdateAll()
  .whenNotMatchedInsertAll()
  .execute()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from bdunm_catalog.ldp_demo.silver_autoloader_simple_scd