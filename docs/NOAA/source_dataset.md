# 01 · The source: NOAA GHCN-Daily on S3

## What it is

Global Historical Climatology Network **Daily** — station-level daily weather
observations from land stations worldwide, some going back to 1763. It is a merged
composite of many national archives, put through a common QC suite. Roughly two
thirds of the stations report precipitation only.

| | |
|---|---|
| Bucket | `s3://noaa-ghcn-pds` |
| Region | `us-east-1` |
| Access | **anonymous read**, no credentials, no requester-pays |
| Update cadence | daily, for the entire period of record |
| Registry | https://registry.opendata.aws/noaa-ghcn/ |
| HTTPS mirror | `https://noaa-ghcn-pds.s3.amazonaws.com/<key>` |

**It is public and anonymous.** 
  Unity Catalog external locations are built around
  a storage credential; there is nothing here to authenticate as. See `docs/NOAA/permissions_model.md`.

## Layout

```
noaa-ghcn-pds/
├── csv/
│   ├── by_station/  ACW00011604.csv …          one file per station, full history
│   └── by_year/     1763.csv … 2026.csv        one file per year, all stations
├── csv.gz/          same two prefixes, gzipped
├── parquet/
│   ├── by_station/  STATION=<id>/*.snappy.parquet
│   └── by_year/     YEAR=<yyyy>/*.snappy.parquet      Hive-partitioned
├── ghcnd-stations.txt      station master  (~11 MB, fixed width)
├── ghcnd-inventory.txt     station × element periods of record (~34 MB, fixed width)
├── ghcnd-countries.txt     FIPS country codes (~4 KB, fixed width)
├── ghcnd-states.txt        US/CA state codes  (~1 KB, fixed width)
├── ghcnd-version.txt       version stamp — no data
├── readme.txt              the authoritative format spec
└── status.txt, mingle-list.txt   NCEI operational chatter — not data
```

`by_station` and `by_year` are the **same observations**, re-sharded. `csv` and
`parquet` are the same observations again, re-encoded. Pick one axis per lab:

* **`parquet/by_year/YEAR=2026/`** → many medium part-files, columnar, typed footer.
* **`csv/by_station/CS*.csv`** → many tiny text files.
  A station id begins with its 2-char FIPS country code, so `CS` = Costa Rica,
  `PM` = Panama, `NU` = Nicaragua. That prefix filter is what keeps this half of
  the lab at a few MB instead of a few hundred GB.

## Schema — daily observations

Identical in `csv/*` and `parquet/*`. One row per **station-day-element**.

| Column | Width | Meaning |
|---|---|---|
| `ID` | 11 | station id; chars 1–2 = FIPS country, char 3 = network code |
| `DATE` | 8 | `YYYYMMDD`, e.g. `20260131` |
| `ELEMENT` | 4 | what was measured (see below) |
| `DATA_VALUE` | 5 | the value, **in the element's own unit** |
| `M_FLAG` | 1 | measurement flag |
| `Q_FLAG` | 1 | quality flag — **blank means it passed QC** |
| `S_FLAG` | 1 | source flag |
| `OBS_TIME` | 4 | observation time `HHMM`, often blank |

The NODD-generated CSVs carry a header row; older copies of this dataset do not.

### The five core elements

| Code | Measure | Unit as stored |
|---|---|---|
| `PRCP` | precipitation | tenths of mm |
| `SNOW` | snowfall | mm |
| `SNWD` | snow depth | mm |
| `TMAX` | daily maximum temperature | **tenths of °C** |
| `TMIN` | daily minimum temperature | **tenths of °C** |

There are dozens more (`TAVG`, `AWND`, `WSF2`, `WT**` weather types, …). Section III
of the bucket's `readme.txt` is the definitive list.

Two things belong in **silver**, not bronze:

* dividing temperature and precipitation by 10 — `TMAX = 251` is 25.1 °C;
* dropping rows where `Q_FLAG` is non-blank — those failed a QC check and are
  published anyway, on purpose.

Bronze keeps both as they came. That is the point of bronze.

## Schema — fixed-width reference files

Positions are 1-based and inclusive, copied from `readme.txt`.

**`ghcnd-stations.txt`**

| Column | Positions |
|---|---|
| `ID` | 1–11 |
| `LATITUDE` | 13–20 |
| `LONGITUDE` | 22–30 |
| `ELEVATION` | 32–37 (metres, `-999.9` = missing) |
| `STATE` | 39–40 |
| `NAME` | 42–71 |
| `GSN_FLAG` | 73–75 |
| `HCN/CRN_FLAG` | 77–79 |
| `WMO_ID` | 81–85 |

**`ghcnd-inventory.txt`** — grain is (station, element)

| Column | Positions |
|---|---|
| `ID` | 1–11 |
| `LATITUDE` | 13–20 |
| `LONGITUDE` | 22–30 |
| `ELEMENT` | 32–35 |
| `FIRSTYEAR` | 37–40 |
| `LASTYEAR` | 42–45 |

**`ghcnd-countries.txt`** and **`ghcnd-states.txt`** — `CODE` 1–2, `NAME` 4–50.

## Rough sizes (check them yourself with the discovery notebook)

| Object | Order of magnitude |
|---|---|
| One recent year, CSV | ~1–2 GB uncompressed |
| One recent year, parquet | a few hundred MB across many part-files |
| Whole `csv/by_year/` | hundreds of GB |
| One `by_station` CSV | tens to hundreds of KB |
| `ghcnd-inventory.txt` | ~34 MB, ~750k rows |
| `ghcnd-stations.txt` | ~11 MB, ~130k rows |

The full archive is far too big for a sandbox, which is why `source_year` exists as
a bundle variable and every acquisition task caps its own batch size.

## Citation

> NOAA Global Historical Climatology Network Daily (GHCN-D) was accessed on
> *DATE* from https://registry.opendata.aws/noaa-ghcn.
