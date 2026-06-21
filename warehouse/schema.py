"""
Warehouse schema — DuckDB (columnar, zero-infra, production-grade for analytics).

Tables
------
raw_lol_matches      : Bronze layer — raw API JSON stored as VARCHAR
raw_cs_matches       : Bronze layer — raw PandaScore JSON
lol_match_stats      : Silver layer — normalised per-player match rows
cs_match_stats       : Silver layer — normalised per-team match rows
lol_player_agg       : Gold layer  — aggregated player KPIs
cs_team_agg          : Gold layer  — aggregated team KPIs
pipeline_runs        : Audit log of every Prefect flow execution
data_quality_checks  : Results of every DQ assertion
"""

import duckdb
import logging
from config.settings import DB_PATH

log = logging.getLogger(__name__)


DDL = """
-- ── Bronze ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_lol_matches (
    match_id        VARCHAR PRIMARY KEY,
    region          VARCHAR,
    raw_json        VARCHAR,           -- full Riot API payload as JSON string
    ingested_at     TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw_cs_matches (
    match_id        BIGINT PRIMARY KEY,
    tournament_name VARCHAR,
    raw_json        VARCHAR,
    ingested_at     TIMESTAMPTZ DEFAULT now()
);

-- ── Silver ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS lol_match_stats (
    row_id          VARCHAR PRIMARY KEY,   -- match_id || '_' || puuid
    match_id        VARCHAR,
    puuid           VARCHAR,
    summoner_name   VARCHAR,
    champion_name   VARCHAR,
    team_position   VARCHAR,
    win             BOOLEAN,
    kills           INTEGER,
    deaths          INTEGER,
    assists         INTEGER,
    kda             DOUBLE,
    cs_per_min      DOUBLE,
    vision_score    INTEGER,
    damage_dealt    INTEGER,
    gold_earned     INTEGER,
    match_duration  INTEGER,             -- seconds
    game_version    VARCHAR,
    match_ts        TIMESTAMPTZ,
    processed_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cs_match_stats (
    row_id          VARCHAR PRIMARY KEY,   -- match_id || '_' || team_id
    match_id        BIGINT,
    team_id         BIGINT,
    team_name       VARCHAR,
    opponent_name   VARCHAR,
    win             BOOLEAN,
    rounds_won      INTEGER,
    rounds_lost     INTEGER,
    score           VARCHAR,
    tournament_name VARCHAR,
    match_ts        TIMESTAMPTZ,
    processed_at    TIMESTAMPTZ DEFAULT now()
);

-- ── Gold ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS lol_player_agg (
    puuid           VARCHAR PRIMARY KEY,
    summoner_name   VARCHAR,
    games_played    INTEGER,
    win_rate        DOUBLE,
    avg_kda         DOUBLE,
    avg_kills       DOUBLE,
    avg_deaths      DOUBLE,
    avg_assists     DOUBLE,
    avg_cs_per_min  DOUBLE,
    avg_vision      DOUBLE,
    top_champion    VARCHAR,
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cs_team_agg (
    team_id         BIGINT PRIMARY KEY,
    team_name       VARCHAR,
    matches_played  INTEGER,
    win_rate        DOUBLE,
    avg_rounds_won  DOUBLE,
    avg_rounds_lost DOUBLE,
    updated_at      TIMESTAMPTZ DEFAULT now()
);

-- ── Audit ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          VARCHAR PRIMARY KEY,
    flow_name       VARCHAR,
    status          VARCHAR,            -- success | failed | skipped
    rows_ingested   INTEGER,
    rows_transformed INTEGER,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    error_msg       VARCHAR
);

CREATE TABLE IF NOT EXISTS data_quality_checks (
    check_id        VARCHAR PRIMARY KEY,
    run_id          VARCHAR,
    table_name      VARCHAR,
    check_name      VARCHAR,
    passed          BOOLEAN,
    metric_value    DOUBLE,
    threshold       DOUBLE,
    checked_at      TIMESTAMPTZ DEFAULT now()
);
"""


def init_warehouse() -> duckdb.DuckDBPyConnection:
    """Create warehouse + all tables if they don't exist yet (writer connection)."""
    con = duckdb.connect(DB_PATH)
    con.execute(DDL)
    log.info("Warehouse initialised at %s", DB_PATH)
    return con


def get_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """
    Open a connection to the warehouse.

    read_only=True is recommended for anything that only queries — the
    Streamlit dashboard, notebooks, ad hoc analysis — since it lets multiple
    readers connect concurrently alongside the Prefect pipeline's writer
    connection without lock contention. Note: read_only has no effect on
    MotherDuck connections (md:...) since MotherDuck handles concurrent
    access natively; it only matters for the local .duckdb file.
    """
    if DB_PATH.startswith("md:"):
        return duckdb.connect(DB_PATH)
    return duckdb.connect(DB_PATH, read_only=read_only)


if __name__ == "__main__":
    logging.basicConfig(level="INFO")
    con = init_warehouse()
    tables = con.execute("SHOW TABLES").fetchall()
    print("Tables created:", [t[0] for t in tables])
    con.close()
