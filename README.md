# NYC Taxi Analytics Engineering

A dbt + DuckDB warehouse built on NYC TLC's real Yellow Taxi trip data (millions of rows per month), designed to demonstrate the things an analyst-only SQL project can't: a layered model architecture, incremental loading, a real SCD Type 2 dimension, data testing, and CI — not just a query that answers one question.

**Business framing:** an analytics team wants a self-serve warehouse for trip and zone-demand analysis, fed by TLC's monthly data drops, where "which zones are trending up or down in demand" is a question the data itself can answer (not just today's snapshot).

## Architecture

```
sources (external Parquet/CSV)
        │
        ▼
  staging/          1:1 with sources, cleaned + typed, incremental load
        │
        ▼
  intermediate/     joins, enrichment, business logic
        │
        ▼
  marts/            what analysts/BI tools actually query
        │
        ▼
  snapshots/        SCD Type 2 history on top of a marts-layer "current state" model
```

![dbt lineage graph](docs/dag.png)

## Design decisions (and why)

**Incremental loading, not a full rebuild every time.** `stg_trips` (`models/staging/stg_trips.sql`) only processes rows with `pickup_datetime` later than what's already loaded. Real production warehouses receive new data continuously, not as one big backfill — so this project is built and tested the same way: load January, run the pipeline, then let February's file land and run again. See [Real results](#real-results-from-running-this-twice) below for what actually happened doing that.

**The SCD Type 2 candidate isn't the zone lookup table.** The obvious "dimension that could have history" is `dim_zones` (TLC's LocationID → borough/zone mapping) — but that table essentially never changes, so snapshotting it would produce one static row per zone forever. Instead, `int_zone_current_demand` computes each zone's **current demand tier** (High/Medium/Low, by trip-count tercile that month) — a value that genuinely changes as new months of real data land — and `snapshots/zone_demand_tier_snapshot.sql` tracks *that* over time. This is the actual business question ("has this zone's demand classification shifted?"), and it's why a `check` strategy snapshot (not `timestamp`) is the right tool: there's no natural `updated_at` column on a derived aggregate.

**Staging filters bad data; it doesn't just flag it.** TLC's raw files reliably contain ~3% physically invalid rows (negative fares, zero/negative trip distance, dropoff before pickup, a stray pre-2009 timestamp). Nothing downstream can meaningfully use a trip with a negative fare, so these are filtered in `stg_trips`, not carried forward. See the code comments in that file for two real bugs this surfaced during development — a duplicate-key collision from paired charge/reversal billing records, and a fully-duplicated raw row in the February file — both caught by the unique-key test failing, not assumed away.

## Real results from running this twice

January loaded first, then February added and the pipeline re-run — the actual incremental/SCD2 behavior this project is meant to demonstrate, not just built once with everything already present:

| | January only | After February added (incremental run) |
|---|---|---|
| Trips in `fct_trips` | 2,870,072 | 5,771,879 |
| Zones with demand data | 258 | 260 |
| "Current" month (by volume, not just latest) | January | February |

**15 of 260 zones (5.8%) changed demand tier between the two months** — e.g. Canarsie, Brooklyn went High → Medium; Bronxdale, Bronx went Low → Medium. Each carries real `dbt_valid_from`/`dbt_valid_to` timestamps in `snapshots.zone_demand_tier_snapshot`, so "which zones are trending up or down" is a query away, not a re-analysis.

An earlier version of the snapshot also tracked `trip_count` and `avg_fare` as check columns, not just `demand_tier` — that produced 252/260 zones "changing" every run, because those figures drift by small amounts for nearly every zone regardless of whether anything meaningful shifted. Narrowing to just `demand_tier` is what makes the snapshot's history actually answer the question it's meant to answer — documented in the snapshot file itself.

## Running it

Requires Python and the [DuckDB CLI](https://duckdb.org/docs/installation) (or just `pip install duckdb` for the CLI too).

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install dbt-core dbt-duckdb
dbt deps
export DBT_PROFILES_DIR=$(pwd)   # profiles.yml lives in this repo, not ~/.dbt/

# Get the data -- see data/README.md for the two-stage (Jan, then Feb) walkthrough
dbt build
dbt docs generate && dbt docs serve   # optional: browse the docs site + DAG
```

## Testing

33 data tests across the DAG: not-null/unique on every primary key, `relationships` tests tying `fct_trips` back to `dim_zones`, `accepted_values` on vendor/payment codes, custom `dbt_utils.expression_is_true` checks on fare and distance, and a `unique_combination_of_columns` test on the zone/month grain. Run `dbt test` on its own, or `dbt build` to run models and tests together in dependency order.

## CI

[![dbt build CI](https://github.com/noahfighter883/nyc-taxi-analytics-engineering/actions/workflows/ci.yml/badge.svg)](https://github.com/noahfighter883/nyc-taxi-analytics-engineering/actions/workflows/ci.yml)

Every push runs `dbt build` **twice** against small synthetic fixtures (`data/sample/`) — once with only a "January" fixture, then again with "February" added — so CI actually exercises the incremental model and the snapshot across two runs, not just proves the SQL compiles. See [.github/workflows/ci.yml](.github/workflows/ci.yml).

## Repo structure

```
data/
  README.md                how to get the real data, and the two-stage load walkthrough
  raw/                      gitignored -- real monthly Parquet files + zone lookup go here
  sample/                   small synthetic fixtures (two months) used by CI
models/
  staging/                  1:1 with sources, cleaned/typed/incremental, tests
  intermediate/             joins, enrichment, the zone-demand-tier logic
  marts/                    fct_trips, dim_zones, dim_dates, mart_zone_demand_summary
snapshots/
  zone_demand_tier_snapshot.sql   the SCD Type 2 history
scripts/
  generate_sample_data.py   regenerates the CI fixtures in data/sample/
docs/
  dag.png                   lineage graph screenshot
.github/workflows/
  ci.yml                    two-stage incremental build on every push
dbt_project.yml, profiles.yml, packages.yml
```

## Tools

dbt-core + dbt-duckdb (no cloud warehouse or credentials needed), dbt_utils, GitHub Actions.
