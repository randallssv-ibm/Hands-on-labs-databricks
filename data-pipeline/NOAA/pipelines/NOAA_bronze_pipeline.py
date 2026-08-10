# Purpose: Bronze declarative pipeline for the NOAA GHCN-D hands-on lab
# Organization: IBM
# Owner: Snowbricks

"""
Bronze layer

Reads from s3://noaa-ghcn-pds.

  ghcnd-stations.txt                -> materialized_view   (full snapshot)
  ghcnd-countries/-states.txt       -> materialized_view   (small, full snapshot)
  ghcnd-inventory.txt               -> AUTO CDC            (upsert on a key)
  parquet/by_year/YEAR=<year>       -> streaming table      (Auto Loader)

A dataset function just returns a DataFrame.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

SOURCE = "s3://noaa-ghcn-pds"
YEAR = spark.conf.get("ghcn.source_year")

TABLE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
}


def _lineage(df):
    return df.withColumn("_source_file", F.col("_metadata.file_path")) \
              .withColumn("_ingested_at", F.current_timestamp())

# A materialized view is recomputed from its source on every pipeline update without writing the overwrite code.
@dp.materialized_view(
    name="bronze_ghcnd_stations",
    comment="Station master list, full snapshot (materialized view). Rebuilt from ghcnd-stations.txt on every run.",
    table_properties=TABLE_PROPERTIES,
)
def bronze_ghcnd_stations():
    raw = spark.read.text(f"{SOURCE}/ghcnd-stations.txt")
    return _lineage(
        raw.select(
            F.trim(F.substring("value", 1, 11)).alias("station_id"),
            F.trim(F.substring("value", 13, 8)).alias("latitude"),
            F.trim(F.substring("value", 22, 9)).alias("longitude"),
            F.trim(F.substring("value", 32, 6)).alias("elevation"),
            F.trim(F.substring("value", 39, 2)).alias("state"),
            F.trim(F.substring("value", 42, 30)).alias("name"),
        ).where(F.length("value") > 0)
    )

# Country + state codes, two tiny files unioned into one table.
@dp.materialized_view(
    name="bronze_ghcnd_code_lists",
    comment="FIPS country codes and US/CA state codes, refreshed on every run.",
    table_properties=TABLE_PROPERTIES,
)
def bronze_ghcnd_code_lists():
    def read_codes(file, code_type):
        raw = spark.read.text(f"{SOURCE}/{file}") 
        return raw.select(
            F.lit(code_type).alias("code_type"),
            F.trim(F.substring("value", 1, 2)).alias("code"),
            F.trim(F.substring("value", 4, 47)).alias("name"),
        ).where(F.length("value") > 0)

    codes = read_codes("ghcnd-countries.txt", "COUNTRY").unionByName(
        read_codes("ghcnd-states.txt", "STATE")
    )
    return _lineage(codes)


# Inventory, upsert on (station_id, element) -> AUTO CDC
@dp.temporary_view(name="inventory_source")
def inventory_source():
    raw = spark.read.text(f"{SOURCE}/ghcnd-inventory.txt")
    return raw.select(
        F.trim(F.substring("value", 1, 11)).alias("station_id"),
        F.trim(F.substring("value", 13, 8)).alias("latitude"),
        F.trim(F.substring("value", 22, 9)).alias("longitude"),
        F.trim(F.substring("value", 32, 4)).alias("element"),
        F.trim(F.substring("value", 37, 4)).alias("first_year"),
        F.trim(F.substring("value", 42, 4)).alias("last_year"),
        F.current_timestamp().alias("_ingested_at"),
    ).where(F.length("value") > 0)

dp.create_streaming_table(
    name="bronze_ghcnd_inventory",
    comment="Current period of record per (station, element). Kept in sync with AUTO CDC.",
    table_properties=TABLE_PROPERTIES,
)

dp.create_auto_cdc_flow(
    target="bronze_ghcnd_inventory",
    source="inventory_source",
    keys=["station_id", "element"],
    sequence_by=F.col("_ingested_at"),
    stored_as_scd_type=1,
)

# Daily parquet observations, incremental file arrivals (Auto Loader).
# This is the pattern a declarative pipeline is built for.
@dp.table(
    name="bronze_ghcnd_daily_parquet",
    comment="Daily observations for YEAR from parquet/by_year, picked up incrementally.",
    table_properties=TABLE_PROPERTIES,
)
@dp.expect("station_id_is_11_chars", "length(ID) = 11")
def bronze_ghcnd_daily_parquet():
    return _lineage(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(f"{SOURCE}/parquet/by_year/YEAR={YEAR}/")
    )