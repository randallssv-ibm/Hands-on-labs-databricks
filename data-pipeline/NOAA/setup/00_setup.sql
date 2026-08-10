-- DBX HANDS ON LABS SETUP

-- Part 1, GHCN-D NOAA_bronze setup

-- catalog: training_dev
-- schema: NOAA_bronze
-- storage credential: ghcn_public_data   (must already exist)

-- Required pre-existing roles: data_engineers, analysts

-- -- CATALOG SHOULD ALREADY EXIST
-- CREATE CATALOG IF NOT EXISTS training_dev;
CREATE SCHEMA  IF NOT EXISTS training_dev.NOAA_bronze;

-- Read access to the public bucket. Everything after this reads
-- s3://noaa-ghcn-pds/... directly with plain spark.read / spark.readStream.
CREATE EXTERNAL LOCATION IF NOT EXISTS ghcn_public_bucket
  URL 's3://noaa-ghcn-pds/'
  WITH (STORAGE CREDENTIAL `ghcn_public_data`)
  COMMENT 'Public NOAA GHCN-D bucket, read-only';

GRANT READ FILES ON EXTERNAL LOCATION ghcn_public_bucket TO `data_engineers`;

-- The one table this project writes to directly 
-- (the declarative pipeline creates and owns its own four tables, don't pre-create those).
CREATE TABLE IF NOT EXISTS training_dev.NOAA_bronze.NOAA_bronze_ghcnd_daily_csv (
  id STRING, 
  date STRING, 
  element STRING, 
  data_value STRING,
  m_flag STRING, 
  q_flag STRING, 
  s_flag STRING, 
  obs_time STRING,
  _source_file STRING, 
  _ingested_at TIMESTAMP
)
COMMENT 'Daily observations, loaded by a plain Structured Streaming job';

-- Only used for a streaming checkpoint.
CREATE VOLUME IF NOT EXISTS training_dev.NOAA_bronze.checkpoints
  COMMENT 'Streaming checkpoint';

-- Grants: engineers read + write, analysts read only.
GRANT USE CATALOG  ON CATALOG training_dev TO `data_engineers`;
GRANT USE CATALOG  ON CATALOG training_dev TO `analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA training_dev.NOAA_bronze TO `data_engineers`;
GRANT USE SCHEMA, SELECT ON SCHEMA training_dev.NOAA_bronze TO `analysts`;
GRANT MODIFY, CREATE TABLE, READ VOLUME, WRITE VOLUME
  ON SCHEMA training_dev.NOAA_bronze TO `data_engineers`;
