# Databricks notebook source
# COMMAND ----------
# Notebook: daily_csv_streaming
# Purpose: Plain Structured Streaming load of GHCN-D daily CSV observations into bronze
# Organization: IBM
# Owner: Snowbricks

# COMMAND ----------
# MAGIC %md
# MAGIC # Daily observations (CSV), plain Structured Streaming
# MAGIC
# MAGIC Same idea as the pipeline's parquet table — Auto Loader picking up new files —
# MAGIC but written by hand instead of inside a pipeline. With the checkpoint, the trigger, and the write.
# MAGIC
# MAGIC Source: `csv/by_station/<STATION>.csv`, filtered to a few countries by filename,
# MAGIC read directly from the bucket. `readStream` for the read, `writeStream` for the
# MAGIC write, `.toTable()` instead of `.saveAsTable()` because this is a stream.

# COMMAND ----------

dbutils.widgets.text("catalog", "training_dev")
dbutils.widgets.text("schema", "NOAA_bronze")
dbutils.widgets.text("country_codes", "CS,PM,NU")  # Costa Rica, Panama, Nicaragua

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
codes = [c.strip() for c in dbutils.widgets.get("country_codes").split(",")]

TABLE = f"{catalog}.{schema}.NOAA_bronze_ghcnd_daily_csv"
CHECKPOINT = f"/Volumes/{catalog}/{schema}/checkpoint_daily_csv"
SCHEMA_LOCATION = f"/Volumes/{catalog}/{schema}/checkpoint_daily_csv/daily_csv_schema"

# COMMAND ----------

from pyspark.sql import functions as F

glob_pattern = "{" + ",".join(f"{c}*.csv" for c in codes) + "}"

stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("header", "true")
    .option("pathGlobFilter", glob_pattern)
    .load("s3://noaa-ghcn-pds/csv/by_station/")
    .select(
        F.col("ID").alias("id"),
        F.col("DATE").alias("date"),
        F.col("ELEMENT").alias("element"),
        F.col("DATA_VALUE").alias("data_value"),
        F.col("M_FLAG").alias("m_flag"),
        F.col("Q_FLAG").alias("q_flag"),
        F.col("S_FLAG").alias("s_flag"),
        F.col("OBS_TIME").alias("obs_time"),
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )
)

# COMMAND ----------

query = (
    stream.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .trigger(availableNow=True) # process one time (for costs)
    .toTable(TABLE)
)
query.awaitTermination()

# COMMAND ----------

display(spark.table(TABLE).groupBy(F.substring("id", 1, 2).alias("country")).count())

# COMMAND ----------

# MAGIC %md
# MAGIC Run this notebook again with nothing new landed in the bucket and it processes
# MAGIC zero rows — the checkpoint remembers which files it already read.