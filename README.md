# NFL Lakehouse (Bronze → Silver → Gold)

End-to-end NFL data platform project that ingests multi-domain NFL datasets (play-by-play, weekly, seasonal, rosters, schedules, lines, win totals, officials, draft picks/values, combine results, and ID mappings), stores raw data in a **Bronze** layer, cleans/enriches data with **PySpark** into **Silver**, and (later) publishes analytics-ready **Gold** marts via **dbt** into a warehouse (Snowflake/Redshift).

**Downstream apps (locked in):**
- **Tableau** dashboards (BI / stakeholder reporting)
- **Streamlit** interactive app (data product / exploration)

---

## Table of Contents
- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Goals](#goals)
- [Datasets](#datasets)
- [Tech Stack](#tech-stack)
- [Repository Layout](#repository-layout)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Configuration](#configuration)
- [Usage](#usage)
  - [Bronze Ingestion](#bronze-ingestion)
  - [Silver Transforms (Planned)](#silver-transforms-planned)
  - [Gold Marts (Planned)](#gold-marts-planned)
- [Data Quality](#data-quality)
- [Partitioning & Incremental Loads](#partitioning--incremental-loads)
- [Downstream Apps](#downstream-apps)
- [Roadmap](#roadmap)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

---

## Project Overview

**NFL Lakehouse** is a realistic, end-to-end data platform project designed to mirror modern analytics engineering practices:
- ingest many NFL domains into a **Bronze** raw layer
- clean, standardize, and enrich into **Silver**
- publish analytics-ready **Gold** marts (facts/dimensions + metrics) to support BI and interactive apps

A key focus is **identifier standardization** (player/team IDs) across disparate sources using **ID mapping**.

---

## Architecture

High-level flow:

1. **Ingest (Bronze)**
   - Pull datasets (currently via `nfl_data_py`)
   - Write to `data/bronze` as partitioned Parquet

2. **Transform (Silver) — Planned**
   - PySpark jobs to clean, standardize schemas, dedupe, and enrich
   - Apply ID mapping to produce consistent `player_id`, `team_id`, etc.
   - Write to `data/silver` as partitioned Parquet

3. **Serve (Gold) — Planned**
   - Load curated Silver outputs to a warehouse (Snowflake/Redshift)
   - Use dbt to build:
     - conformed **dimensions** (players, teams, seasons, games)
     - canonical **facts** (plays, weekly stats, lines, etc.)
     - tests + docs + lineage

4. **Consume**
   - **Tableau** dashboards (stakeholder reporting)
   - **Streamlit** app (interactive exploration)

---

## Goals

- Build a realistic data platform with **partitioning, incremental loads, data quality gates, orchestration, CI/CD**
- Standardize player/team identifiers using **ID mapping** across sources
- Produce curated facts/dimensions that power both Tableau and Streamlit

---

## Datasets

Planned multi-domain coverage includes:
- Play-by-play
- Weekly and seasonal stats
- Rosters
- Schedules
- Betting lines + win totals
- Officials
- Draft picks + draft values
- Combine results
- ID mappings (crosswalk across sources)

**Current status:** ✅ Bronze schedules ingestion is implemented.

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
