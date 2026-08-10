# NOAA GHCN-D → Bronze

A small Databricks Asset Bundle that reads NOAA's public weather dataset
(`s3://noaa-ghcn-pds`) straight into a bronze layer, using a different read/write
technique per source — chosen for whichever is genuinely the better fit, not
duplicated across two tools.

| Source | Technique | Where |
|---|---|---|
| `ghcnd-stations.txt` | `spark.read` + materialized view | pipeline |
| `ghcnd-countries.txt` / `-states.txt` | `spark.read` + materialized view | pipeline |
| `ghcnd-inventory.txt` | AUTO CDC (upsert on a key) | pipeline |
| `parquet/by_year/YEAR=<year>` | `readStream` + Auto Loader | pipeline |
| `csv/by_station/<CC>*.csv` | `readStream` + `writeStream` + `.toTable()` | plain job |

No downloads, no boto3, no libraries to install. Every read is a direct
`spark.read` / `spark.readStream` call against `s3://noaa-ghcn-pds/...`.

## One-time AWS step

Unity Catalog needs a **storage credential** to read an external bucket — even a
public one. This is a one-time setup by whoever manages your AWS account, done
once per workspace, not part of this project's code:

1. In AWS, create an IAM role. It needs **no attached permissions** — the bucket
   is public, so nothing needs to authorize access to it. It only needs a trust
   policy allowing Databricks' account to assume it.
2. In Databricks: **Catalog → External Data → Credentials → Create credential**,
   type "AWS IAM Role", paste the role ARN. Name it (default expected here:
   `ghcn_public_data`).

Full click-by-click steps: [Databricks docs — connect to an AWS S3 external
location](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3/).
Once this exists, `data-pipeline/NOAA/setup/setup.sql` does everything else.

## Deploy

```bash
databricks bundle deploy
databricks bundle run bronze_ingest
```

That one job does three things: runs setup, builds the four pipeline tables,
and streams the CSV table. Everything is idempotent — run it again anytime.

`dev` is the default target (paused schedule, your identity). Add `-t prod` to
deploy with the schedule live, running as a service principal.

## Configure

Edit in `databricks.yml`:

| Variable | |
|---|---|
| `workspace.host` | your workspace URL |
| `storage_credential` | name from the AWS step above |
| `de_group` / `analyst_group` | your account groups |
| `run_as_sp` | prod service principal's application ID |
| `source_year` | which year of daily data to load |

## Why this tool for this source

- **Materialized view** for the two reference files — they're full snapshots
  every time, small enough to just rebuild. That's exactly what a materialized
  view does, without writing the overwrite yourself.
- **AUTO CDC** for inventory — it's an upsert on `(station_id, element)`. Hand-
  written, that's a dedup step, a change-predicate, and a soft-delete clause.
  Declaratively, it's five arguments.
- **Streaming table (pipeline)** for the parquet feed — many files arriving
  over time is the exact case a declarative pipeline is built for: managed
  checkpoint, managed schema handling, triggered runs.
- **Plain `readStream`/`writeStream`** for the CSV feed — the same Auto Loader
  engine, but written out by hand so you see what the pipeline was managing for
  you: the checkpoint path, the trigger, the table write.

## Permissions

Two groups, both inheriting from the schema — nothing granted per table:

| | `data_engineers` | `analysts` |
|---|---|---|
| Catalog | `USE CATALOG` | `USE CATALOG` |
| Schema | `USE SCHEMA`, `SELECT`, `MODIFY`, `CREATE TABLE` | `USE SCHEMA`, `SELECT` |
| Checkpoint volume | `READ VOLUME`, `WRITE VOLUME` | — |
| Public bucket | `READ FILES` on the external location | — |

In `prod`, the job and pipeline run as the service principal (`run_as` in
`databricks.yml`), so `MODIFY`/`CREATE TABLE` for the DE group is really a
debugging safety net, not the normal write path.

## Data citation

> NOAA Global Historical Climatology Network Daily (GHCN-D), accessed from
> https://registry.opendata.aws/noaa-ghcn.

## Where to take it next