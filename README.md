# NFL Lakehouse

An end-to-end data engineering platform for NFL analytics, built on the **Bronze → Silver → Gold** medallion architecture. Ingests multi-domain NFL datasets, cleans and standardizes them with PySpark, and orchestrates all pipelines with Dagster — with a Gold layer (dbt + cloud warehouse) and visualization layer (Tableau + Streamlit) planned.

---

## Architecture

```
nflreadpy
    │
    ▼
Polars (staging)
    │  temp Parquet
    ▼
Bronze Layer          ← raw, partitioned by season
    │  PySpark jobs
    ▼
Silver Layer          ← cleaned, deduplicated, standardized
    │  dbt (planned)
    ▼
Gold Layer            ← fact/dimension tables in Snowflake/Redshift (planned)
    │
    ▼
Tableau + Streamlit   (planned)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | `nflreadpy` / `nfl_data_py` |
| Staging | Polars → Snappy Parquet |
| Transformation | PySpark 4.x (local mode) |
| Orchestration | Dagster |
| Storage format | Apache Parquet, partitioned by `season` |
| Warehouse (planned) | Snowflake or Redshift |
| Modeling (planned) | dbt (tests, docs, lineage) |
| Data quality (planned) | Great Expectations or Soda |
| Visualization (planned) | Tableau + Streamlit |

---

## Datasets

| Dataset | Bronze | Silver | Notes |
|---|---|---|---|
| Schedules | ✅ | ✅ | Keyed on `game_id` |
| Play-by-play | ✅ | ✅ | Keyed on `game_id` + `play_id` |
| Rosters | ✅ | ✅ | Keyed on `player_id` (renamed from `gsis_id`); deduped by completeness score |
| Weekly stats | | | Planned |
| Lines / win totals | | | Planned |
| Officials | | | Planned |
| Draft picks / values | | | Planned |
| Combine results | | | Planned |
| ID mappings | | | Planned — standardize player/team IDs across sources |

---

## Repository Layout

```
src/nfl_lakehouse/
├── common/          # Spark session factory, Bronze/Silver I/O helpers
├── ingest/          # Bronze ingestion scripts (one per dataset)
├── spark/           # PySpark Silver cleaning jobs (one per dataset)
├── sources/         # nflreadpy wrappers with version compatibility
└── orchestration/   # Dagster job and op definitions

data/
├── bronze/          # Raw partitioned Parquet (generated, gitignored)
└── silver/          # Cleaned partitioned Parquet (generated, gitignored)
```

---

## Pipelines

Each dataset follows the same two-op pattern, orchestrated by Dagster:

1. **Ingest op** — pulls data via `nflreadpy`, stages as Polars Parquet, then writes to Bronze via Spark (partitioned by `season`)
2. **Clean op** — reads Bronze with Spark, enforces types and key constraints, deduplicates, normalizes strings, and writes to Silver

Jobs are configurable by `season` via the Dagster WebUI or YAML.

**Silver transformations include:**
- Primary key enforcement and null filtering
- Type casting (season, week, play IDs, yardage fields)
- String normalization (team abbreviations, positions → UPPERCASE)
- Deduplication — rosters use a completeness-score window function to keep the most complete row per player/season

---

## Quickstart

```bash
pip install -e ".[dev]"
dagster dev
```

Open `http://localhost:3000` and run any of the three jobs with a `season` config (e.g. `2024`).

---

## Roadmap

- ✅ Bronze ingestion: schedules, PBP, rosters
- ✅ Silver cleaning: schedules, PBP, rosters
- ✅ Dagster orchestration for all three pipelines
- Additional datasets: weekly stats, lines, officials, draft, combine, ID mappings
- Gold layer: dbt models, tests, and docs published to Snowflake/Redshift
- Data quality gates: automated checks on Bronze → Silver transitions
- CI/CD: pipeline validation on PRs
- Tableau dashboards + Streamlit app powered by Gold marts
