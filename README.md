# NFL Lakehouse (Bronze → Silver → Gold)

End-to-end NFL data platform project that ingests multi-domain NFL datasets (play-by-play, weekly, seasonal, rosters, schedules, lines, win totals, officials, draft picks/values, combine results, and ID mappings), stores raw data in a **Bronze** layer, cleans/enriches data with **PySpark** into **Silver**, and (later) publishes analytics-ready **Gold** marts via **dbt** into a warehouse (Snowflake/Redshift).

**Downstream apps (locked in):**
- **Tableau** dashboards (BI / stakeholder reporting)
- **Streamlit** interactive app (data product / exploration)

---

## Goals
- Build a realistic data platform with **partitioning, incremental loads, data quality gates, orchestration, CI/CD**
- Standardize player/team identifiers using **ID mapping** across sources
- Produce curated facts/dimensions that power both Tableau and Streamlit

---

## Tech Stack

### Current
- **Ingest:** `nfl_data_py`
- **Local storage (dev):** Parquet (`data/bronze`)
- **Language:** Python

### Planned (next milestones)
- **Transform (Silver):** PySpark
- **Orchestration:** Dagster or Airflow
- **Warehouse (Gold):** Snowflake or Redshift
- **Modeling:** dbt (tests + docs + lineage)
- **Data quality:** Great Expectations or Soda
- **Visualization / App:** Tableau + Streamlit

---

## Repository Layout
- nfl-lakehouse/
- src/nfl_lakehouse/
- common/ # shared helpers (I/O, config)
- ingest/ # ingestion scripts (Bronze)
- spark/ # PySpark jobs (Silver) [planned]
- data/
- bronze/ # raw datasets (partitioned) - generated
- silver/ # cleaned datasets (partitioned) - generated

## Roadmap (Milestones)

- ✅ Bronze: schedules ingestion
- Silver: PySpark cleaning + standardization
- Add more datasets: pbp, weekly, rosters, lines, draft, combine, officials, ID mappings
- Orchestration: Dagster/Airflow end-to-end runs
- Gold: warehouse + dbt marts/metrics + docs/lineage
- Data quality + CI/CD: automated tests/checks on PRs
- Tableau dashboards + Streamlit app powered by Gold marts