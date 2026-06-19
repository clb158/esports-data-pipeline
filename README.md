# VG Analytics Pipeline
### Video Game Performance Analytics — League of Legends & Counter-Strike 2

A production-grade data engineering portfolio project built to demonstrate
real-world ETL skills for data engineering roles.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Data Sources                                 │
│  Riot Games API (LoL Match-v5)     PandaScore API (CS2 Pro Matches) │
└────────────────┬───────────────────────────────┬────────────────────┘
                 │                               │
                 ▼                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Prefect Orchestration Layer                        │
│  lol_ingest_flow (every 6h)     cs_ingest_flow (every 6h +30m)     │
│                      transform_flow (every 12h)                      │
└────────────────┬───────────────────────────────┬────────────────────┘
                 │                               │
                 ▼                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    DuckDB Data Warehouse                              │
│                                                                      │
│  Bronze (raw JSON)   Silver (normalised rows)   Gold (aggregates)   │
│  ─────────────────   ──────────────────────     ─────────────────   │
│  raw_lol_matches  → lol_match_stats          → lol_player_agg       │
│  raw_cs_matches   → cs_match_stats           → cs_team_agg          │
│                                                                      │
│  Audit: pipeline_runs | data_quality_checks                          │
└────────────────────────────────────────────────────────────────────-┘
```

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| Orchestration | **Prefect 3** | Production scheduler with built-in UI, retries, observability |
| Warehouse | **DuckDB** | Columnar, zero-infra, SQL-native, blazing for analytics |
| Transform | **Python + SQL** | Idempotent medallion architecture (Bronze → Silver → Gold) |
| Data Quality | **Custom DQ tasks** | Row count, null rate, duplicate detection, range checks, freshness |
| Testing | **pytest** | 20 unit tests covering transforms and DQ assertions |
| Source (LoL) | **Riot Match-v5 API** | Official Riot endpoint, rate-limit-aware with backoff |
| Source (CS2) | **PandaScore API** | Free tier, 1 000 req/hr |

## Project Structure

```
vgpipeline/
├── config/
│   └── settings.py          # All config — API keys, crons, thresholds
├── warehouse/
│   └── schema.py            # DDL + init_warehouse() + get_connection()
├── tasks/
│   ├── lol_ingest.py        # Riot API extract tasks
│   ├── cs_ingest.py         # PandaScore extract tasks
│   ├── transforms.py        # Bronze → Silver → Gold transform tasks
│   └── data_quality.py      # Reusable DQ assertion tasks
├── flows/
│   └── pipeline_flows.py    # Prefect flow definitions + scheduling
├── tests/
│   └── test_pipeline.py     # 20 pytest unit tests
├── demo_seed.py             # Synthetic data seed (no API key needed)
└── README.md
```

## Quick Start

### 1. Clone & install dependencies

```bash
git clone <your-repo>
cd vgpipeline
pip install prefect pandas requests duckdb pyarrow pytest
```

### 2. (Optional) Set API keys

```bash
export RIOT_API_KEY="RGAPI-your-key-here"        # https://developer.riotgames.com/
export PANDASCORE_TOKEN="your-token-here"         # https://pandascore.co/
export RIOT_REGION="na1"                          # na1 | euw1 | kr | ...
```

Without API keys, the pipeline runs in demo mode. Use `demo_seed.py` to
populate the warehouse with realistic synthetic data.

### 3. Seed the warehouse (no API key needed)

```bash
python demo_seed.py
```

### 4. Run the pipeline manually

```bash
python -m flows.pipeline_flows        # runs all three flows once, locally
```

### 5. Start the Prefect UI + deploy schedules

```bash
prefect server start &                 # opens http://127.0.0.1:4200
python -m flows.pipeline_flows --deploy
```

### 6. Run tests

```bash
python -m pytest tests/ -v            # 20 tests, ~2 seconds
```

---

## Key Engineering Patterns

### Medallion Architecture
Raw JSON is always preserved in the Bronze layer. Transforms are idempotent —
re-running them never double-counts. Only unprocessed Bronze rows are promoted
to Silver on each run.

### Idempotent Upserts
Every ingest task checks for existing `match_id` before inserting, using
`INSERT OR REPLACE` or `INSERT OR IGNORE`. Safe to re-run after failures.

### Rate-Limit Awareness
Riot API enforces 20 req/sec and 100 req/2min on dev keys. The ingest tasks
respect `Retry-After` headers and sleep between requests at ~50 ms/req.

### Data Quality Gates
Every transform flow runs a battery of checks after loading:
- **Row count** — table must have ≥ N rows
- **Null rate** — critical columns must be < 10 % null
- **Duplicate detection** — primary keys must be unique
- **Value ranges** — KDA ∈ [0, 100], win_rate ∈ [0, 1], etc.
- **Freshness** — data must be < 24 hrs old

Results are persisted to `data_quality_checks` for audit and trending.

### Audit Logging
Every flow execution writes a row to `pipeline_runs` with status, row counts,
timestamps, and error messages — enabling pipeline health dashboards.

---

## Metrics Produced (Gold Layer)

### `lol_player_agg` — per-player KPIs
| Column | Description |
|---|---|
| `win_rate` | Percentage of matches won |
| `avg_kda` | Mean (K + A) / max(D, 1) across all matches |
| `avg_cs_per_min` | Mean creep score rate (proxy for farming efficiency) |
| `avg_vision` | Mean vision score (ward utility) |
| `top_champion` | Most-played champion (MODE aggregate) |

### `cs_team_agg` — per-team KPIs
| Column | Description |
|---|---|
| `win_rate` | Match win percentage |
| `avg_rounds_won` | Average rounds won per map |
| `avg_rounds_lost` | Average rounds lost per map |

---

## Scheduling (Production)

| Flow | Cron | Description |
|---|---|---|
| `lol-ingest-flow` | `0 */6 * * *` | Pull LoL match data every 6 hours |
| `cs-ingest-flow` | `30 */6 * * *` | Pull CS2 data every 6 hrs (offset 30m) |
| `transform-flow` | `0 */12 * * *` | Bronze→Gold + DQ checks every 12 hours |

---

## Extending the Pipeline

- **Add a summoner to the watch list** → update `WATCH_LIST` in `flows/pipeline_flows.py`
- **Add a new DQ check** → add a `@task` function in `tasks/data_quality.py`
- **Add a new game** → create `tasks/<game>_ingest.py` and `tasks/<game>_transforms.py`, add a flow in `flows/pipeline_flows.py`
- **Scale to cloud** → swap `duckdb.connect(file)` for MotherDuck; swap local Prefect server for Prefect Cloud

---

## Relevant Skills Demonstrated

`Python` · `SQL` · `DuckDB` · `Prefect` · `REST API Integration`
`ETL / ELT` · `Medallion Architecture` · `Data Quality` · `pytest`
`Rate Limit Handling` · `Idempotent Pipelines` · `Audit Logging`
