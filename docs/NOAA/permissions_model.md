# 03 · Permission model

Unity Catalog governance for the bronze layer, differentiated by environment.
Applied by `setup.sql`.

## Principals

| Principal | Type | Who |
|---|---|---|
| `platform_admins` | account group | **owns** the catalog and schema. Owners need no grants — ownership already implies everything. |
| `dbx_labs_data_engineers` | account group | builds and operates the pipelines |
| `dbx_labs_analysts` | account group | consumes the data |
| `sp_platform_admin` | service principal | runs every production job (`run_as` in the `prod` target) |

Create them at the account level before the first deploy, then set
`de_group` / `analyst_group` / `admin_group` / `run_as_sp` in `databricks.yml`.

**Ownership goes to a group, never a person.** An individual owner is a resignation
away from an orphaned catalog that only a metastore admin can unstick.

## Grant matrix

Privileges in Unity Catalog **inherit downward**: a grant on a schema applies to
every table and volume in it, present and future. Everything below is granted at
catalog, schema or volume level; nothing is granted per table, which is what keeps
the model reviewable.

### Catalog

| Privilege | DE | dbx_labs_analysts | Service principal |
|---|:--:|:--:|:--:|
| `USE CATALOG` | ✅ | ✅ | ✅ |
| `BROWSE` | ✅ | ✅ | — |

`USE CATALOG` is a traversal right, not a read right — on its own it exposes
nothing. `BROWSE` lets a person see names and comments in the catalog explorer
without being able to query, which is how you make a data catalog discoverable
without leaking rows.

### Bronze schema

| Privilege | DE (dev) | DE (prod) | dbx_labs_analysts (dev) | dbx_labs_analysts (prod) | SP |
|---|:--:|:--:|:--:|:--:|:--:|
| `USE SCHEMA` | ✅ | ✅ | ✅ | — | ✅ |
| `SELECT` | ✅ | ✅ | ✅ | — | ✅ |
| `MODIFY` | ✅ | — | — | — | ✅ |
| `CREATE TABLE` | ✅ | — | — | — | ✅ |
| `CREATE VOLUME` | ✅ | — | — | — | — |
| `CREATE FUNCTION` | ✅ | — | — | — | — |
| `EXECUTE` | ✅ | — | — | — | — |
| `REFRESH` | ✅ | — | — | — | ✅ |

### Volumes

| Privilege | DE (dev) | DE (prod) | dbx_labs_analysts | SP |
|---|:--:|:--:|:--:|:--:|
| `checkpoint` — `READ VOLUME` | ✅ | ✅ | — | ✅ |
| `checkpoint` — `WRITE VOLUME` | ✅ | — | — | ✅ |

## Why each line is there

**Engineers write in dev, read in prod.** This is the single most important line in
the model. In dev they own the loop end to end — create tables, drop them, iterate.
In prod they keep `SELECT` and `READ VOLUME` so they can diagnose a 3 a.m. failure
without a break-glass request, but they cannot write. Every production byte arrives
through a deployed job running as the service principal, which means every change is
in git and every write is attributable. `MODIFY` in prod is the privilege that
quietly turns a pipeline into a pile of manual fixes.

**dbx_labs_analysts see bronze in dev and nothing in prod.** Bronze here is raw on purpose:
temperatures in tenths of a degree, rows that failed QC published anyway, no dedup.
Handing that to a BI team in production produces confidently wrong dashboards. In
dev, exploring it is exactly how an analyst learns why silver exists. In prod they
are pointed at silver/gold, which this lab leaves as an exercise.

**Nobody but the pipeline touches `checkpoint`.** Checkpoints and Auto Loader schema
locations are pipeline internals. Someone with `WRITE VOLUME` on `ops` can "fix" a
stuck stream by deleting a checkpoint folder, and the next run silently reloads the
entire landing zone.

**No table-level grants.** Every privilege lands on catalog, schema or volume, so
"who can read bronze?" is one `SHOW GRANTS`, not a scan of eight tables. Per-table
exceptions are where permission models go to rot.

## Job permissions (bundle level)

Set in the `prod` target of `databricks.yml` and applied to every job in the bundle:

| Level | Group | Meaning |
|---|---|---|
| `CAN_MANAGE` | `platform_admins` | edit, delete, change permissions |
| `CAN_MANAGE_RUN` | `dbx_labs_data_engineers` | trigger and cancel runs; **cannot** edit the definition |
| `CAN_VIEW` | `dbx_labs_analysts` | see runs, logs and outcomes |

`CAN_MANAGE_RUN` is the deliberate one: engineers can re-run a failed load at 3 a.m.
without being able to edit the job in the UI and drift it away from the bundle. In
`dev`, `mode: development` already gives the deploying user sole ownership of their
own prefixed copies, so no permission block is needed there.

## What the two targets actually change

| | `dev` | `prod` |
|---|---|---|
| Catalog | `training_dev` | `training_prod` |
| Job names | prefixed with your short name | clean |
| Schedules | paused | **unpaused** |
| `run_as` | the deploying user | `sp_platform_admin` |
| Job permissions | implicit (owner only) | explicit three-tier block |
| Bronze writes | data engineers | service principal only |

Same code, same resource definitions, different blast radius. That is the whole
argument for asset bundles in one table.

## Storage credentials — if you point this at a private bucket

This lab reads a public bucket with `boto3` in unsigned mode, so no storage
credential is involved. Swap the source for a private one and you add a layer:

1. A **storage credential** (IAM role / managed identity) — owned by
   `platform_admins`, granted to nobody directly.
2. An **external location** over the prefix — grant `READ FILES` to
   `dbx_labs_data_engineers` and the service principal; grant `WRITE FILES` to almost
   no one.
3. Never `CREATE EXTERNAL LOCATION` for the engineering group. That privilege lets
   the holder define a location over any path the credential can reach, which
   quietly routes around every grant above it.

## Verification

Run after any deploy:

```sql
SHOW GRANTS ON CATALOG training_prod;
SHOW GRANTS ON SCHEMA  training_prod.NOAA_bronze;
SHOW GRANTS ON VOLUME  training_prod.NOAA_bronze.checkpoint;

DESCRIBE SCHEMA EXTENDED training_prod.NOAA_bronze;   -- confirm the owner is the admin group
```

And the checks that matter more than any matrix — as a member of
`dbx_labs_data_engineers`, against **prod**:

```sql
SELECT count(*) FROM training_prod.NOAA_bronze.bronze_ghcnd_stations;   -- should succeed
DELETE FROM training_prod.NOAA_bronze.bronze_ghcnd_stations WHERE 1=0;  -- should be denied
```
