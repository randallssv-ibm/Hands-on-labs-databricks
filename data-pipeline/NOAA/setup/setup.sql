-- DBX HANDS ON LABS SETUP

-- Part 1, GHCN-D NOAA_bronze setup

-- catalog: training_dev
-- schema: NOAA_bronze
-- storage credential: ghcn_public_data   (must already exist)

-- Required pre-existing roles for all: dbx_labs_data_engineers, dbx_labs_analysts

-- -- CATALOG SHOULD ALREADY EXIST
-- CREATE CATALOG IF NOT EXISTS training_dev;
CREATE SCHEMA  IF NOT EXISTS training_dev.NOAA_bronze
COMMENT 'Bronze layer for the NOAA GHCN-D hands-on lab';

ALTER SCHEMA training_dev.NOAA_bronze SET TAGS (
    'client' = 'na',
    'project' = 'ghcn_bronze_lab',
    'purpose' = 'hands_on_training_bronze_ingestion',
    'tech_owner' = '<full_name>'
);

-- Read access to the public bucket. Everything after this reads
-- s3://noaa-ghcn-pds/... directly with plain spark.read / spark.readStream.
-- CREATE EXTERNAL LOCATION IF NOT EXISTS ghcn_public_bucket
--   URL 's3://noaa-ghcn-pds/'
--   WITH (STORAGE CREDENTIAL `AWS_NOAA`)
--   COMMENT 'Public NOAA GHCN-D bucket, read-only';
-- DUE TO LIMITATIONS FOR READ ONLY EXTERNAL LOCATIONS, THIS MUST BE CREATED ON UI
-- Catalog → Connect → External Locations → Create external location → Manual

GRANT READ FILES ON EXTERNAL LOCATION ghcn_public_bucket TO `dbx_labs_data_engineers`;

-- The one table this project writes to directly
-- (the declarative pipeline creates and owns its own four tables, don't pre-create those).
CREATE TABLE IF NOT EXISTS training_dev.NOAA_bronze.NOAA_bronze_ghcnd_daily_csv (
  id           STRING    COMMENT 'Station id, first 2 chars are the FIPS country code',
  date         STRING    COMMENT 'Observation date, YYYYMMDD',
  element      STRING    COMMENT 'Measurement code, e.g. TMAX, PRCP',
  data_value   STRING    COMMENT 'Raw value in the element''s own unit, untyped',
  m_flag       STRING    COMMENT 'Measurement flag',
  q_flag       STRING    COMMENT 'Quality flag — blank means it passed QC',
  s_flag       STRING    COMMENT 'Source flag',
  obs_time     STRING    COMMENT 'Observation time HHMM, often blank',
  _source_file STRING    COMMENT 'Lineage: source file path',
  _ingested_at TIMESTAMP COMMENT 'Lineage: load timestamp'
)
COMMENT 'Daily observations, loaded by a plain Structured Streaming job';

ALTER TABLE training_dev.NOAA_bronze.NOAA_bronze_ghcnd_daily_csv SET TAGS (
    'file_zone' = 'landing',
    'data_layer' = 'bronze',
    'contains_pii' = 'no',
    'data_classification' = 'public',
    'external_access' = 'yes'
);

ALTER TABLE training_dev.NOAA_bronze.NOAA_bronze_ghcnd_daily_csv SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.dataSkippingStatsColumns' = 'id,date,element',
    'delta.logRetentionDuration' = 'interval 30 days',
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
);

-- Only used for a streaming checkpoint.
CREATE VOLUME IF NOT EXISTS training_dev.NOAA_bronze.checkpoint_daily_csv
  COMMENT 'Streaming checkpoint for daily_csv_streaming.py';

ALTER VOLUME training_dev.NOAA_bronze.checkpoint_daily_csv SET TAGS (
    'file_zone' = 'checkpoint',
    'data_layer' = 'bronze',
    'contains_pii' = 'no',
    'data_classification' = 'internal',
    'purpose' = 'structured_streaming_checkpoint',
    'tech_owner' = '<full_name>'
);

-- Grants: engineers read + write, dbx_labs_analysts read only.
GRANT USE CATALOG  ON CATALOG training_dev TO `dbx_labs_data_engineers`;
GRANT USE CATALOG  ON CATALOG training_dev TO `dbx_labs_analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA training_dev.NOAA_bronze TO `dbx_labs_data_engineers`;
GRANT USE SCHEMA, SELECT ON SCHEMA training_dev.NOAA_bronze TO `dbx_labs_analysts`;
GRANT MODIFY, CREATE TABLE, READ VOLUME, WRITE VOLUME
  ON SCHEMA training_dev.NOAA_bronze TO `dbx_labs_data_engineers`;