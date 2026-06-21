"""
Prefect flows — the orchestration layer.

One deployable entrypoint, `main()`, calls three subflows in sequence:
  lol_ingest_flow     : ingest LoL match data
  cs_ingest_flow      : ingest CS2 match data
  transform_flow      : bronze → silver → gold transforms + DQ checks

Each subflow still appears individually in the Prefect UI with its own
logs and run history — `main()` just wires them together as one unit so
the whole pipeline can be deployed and scheduled with a single command.

Run locally:
  python -m flows.pipeline_flows            # runs main() once

Deploy (single entrypoint, industry-standard pattern):
  uvx prefect-cloud deploy flows/pipeline_flows.py:main \\
    --name esports-pipeline-production \\
    --from https://github.com/clb158/esports-data-pipeline \\
    --env RIOT_API_KEY=... --env PANDASCORE_TOKEN=...
"""

import logging
import sys
import uuid
from datetime import datetime, timezone

from prefect import flow

from config.settings import (
    LOL_INGEST_CRON,
    CS_INGEST_CRON,
    TRANSFORM_CRON,
    NULL_THRESHOLD,
    ROW_MIN,
)
from warehouse.schema import init_warehouse, get_connection
from tasks.lol_ingest import extract_lol_batch, load_raw_lol_matches
from tasks.cs_ingest import extract_cs_matches, load_raw_cs_matches
from tasks.transforms import (
    transform_lol_silver,
    transform_cs_silver,
    aggregate_lol_gold,
    aggregate_cs_gold,
)
from tasks.data_quality import (
    check_row_count,
    check_null_rate,
    check_no_duplicates,
    check_value_range,
    check_freshness,
    all_checks_passed,
)

logging.basicConfig(level="INFO", format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger(__name__)

# Summoner watch-list — swap these for real accounts
WATCH_LIST = [
    {"name": "Hide on bush",   "tag": "KR1",  "region": "kr"},    # KR
    {"name": "Rekkles",      "tag": "1996", "region": "euw"},   # EUW
    {"name": "G2 Caps",    "tag": "1323", "region": "euw"},    # EUW
    {"name": "Caedrel", "tag": "sally", "region": "euw"},   # EUW
]


def _log_run(con, run_id, flow_name, status, rows_in=0, rows_out=0, error=""):
    con.execute("""
        INSERT OR REPLACE INTO pipeline_runs
            (run_id, flow_name, status, rows_ingested, rows_transformed,
             started_at, finished_at, error_msg)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [run_id, flow_name, status, rows_in, rows_out,
          datetime.now(timezone.utc), datetime.now(timezone.utc), error])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Flow 1: LoL Ingest
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@flow(
    name="lol-ingest-flow",
    description="Extract LoL match data from Riot API → load to bronze warehouse layer.",
    log_prints=True,
)
def lol_ingest_flow(summoners: list[dict] | None = None, max_per_player: int = 20):
    """Ingest League of Legends ranked match data."""
    run_id = str(uuid.uuid4())
    init_warehouse()
    con = get_connection()

    summoners = summoners or WATCH_LIST
    log.info("Starting LoL ingest flow for %d summoners", len(summoners))

    try:
        matches = extract_lol_batch(summoners=summoners, max_matches_each=max_per_player)
        rows_in = load_raw_lol_matches(matches=matches, con=con)
        _log_run(con, run_id, "lol-ingest-flow", "success", rows_in=rows_in)
        log.info("✅ LoL ingest complete — %d new matches loaded", rows_in)
    except Exception as exc:
        _log_run(con, run_id, "lol-ingest-flow", "failed", error=str(exc))
        log.exception("❌ LoL ingest flow failed")
        raise
    finally:
        con.close()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Flow 2: CS2 Ingest
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@flow(
    name="cs-ingest-flow",
    description="Extract CS2 pro match data from PandaScore → load to bronze layer.",
    log_prints=True,
)
def cs_ingest_flow(count: int = 100):
    """Ingest Counter-Strike 2 professional match data."""
    run_id = str(uuid.uuid4())
    init_warehouse()
    con = get_connection()

    log.info("Starting CS2 ingest flow — fetching up to %d matches", count)

    try:
        matches = extract_cs_matches(count=count)
        rows_in = load_raw_cs_matches(matches=matches, con=con)
        _log_run(con, run_id, "cs-ingest-flow", "success", rows_in=rows_in)
        log.info("✅ CS2 ingest complete — %d new matches loaded", rows_in)
    except Exception as exc:
        _log_run(con, run_id, "cs-ingest-flow", "failed", error=str(exc))
        log.exception("❌ CS2 ingest flow failed")
        raise
    finally:
        con.close()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Flow 3: Transform + DQ
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@flow(
    name="transform-flow",
    description="Bronze → Silver → Gold transforms with data quality checks.",
    log_prints=True,
)
def transform_flow():
    """
    Full transform pipeline:
      1. LoL: bronze → silver (normalise per-participant rows)
      2. CS2: bronze → silver (normalise per-team rows)
      3. LoL: silver → gold (player KPI aggregates)
      4. CS2: silver → gold (team KPI aggregates)
      5. Data quality checks on all silver + gold tables
    """
    run_id = str(uuid.uuid4())
    init_warehouse()
    con = get_connection()

    log.info("Starting transform flow (run_id=%s)", run_id)
    dq_results = []

    try:
        # ── Silver transforms ──────────────────────────────────────────────
        lol_silver_rows = transform_lol_silver(con=con)
        cs_silver_rows  = transform_cs_silver(con=con)

        # ── Gold aggregations ──────────────────────────────────────────────
        aggregate_lol_gold(con=con)
        aggregate_cs_gold(con=con)

        # ── Data quality — silver ──────────────────────────────────────────
        if lol_silver_rows > 0:
            dq_results += [
                check_row_count(con, "lol_match_stats", run_id, minimum=ROW_MIN),
                check_null_rate(con, "lol_match_stats", "puuid", run_id, NULL_THRESHOLD),
                check_null_rate(con, "lol_match_stats", "match_id", run_id, NULL_THRESHOLD),
                check_no_duplicates(con, "lol_match_stats", "row_id", run_id),
                check_value_range(con, "lol_match_stats", "kda", run_id, lo=0.0, hi=100.0),
                check_value_range(con, "lol_match_stats", "cs_per_min", run_id, lo=0.0, hi=30.0),
            ]

        if cs_silver_rows > 0:
            dq_results += [
                check_row_count(con, "cs_match_stats", run_id, minimum=ROW_MIN),
                check_null_rate(con, "cs_match_stats", "team_id", run_id, NULL_THRESHOLD),
                check_no_duplicates(con, "cs_match_stats", "row_id", run_id),
                check_value_range(con, "cs_match_stats", "rounds_won", run_id, lo=0.0),
            ]

        # ── Data quality — gold ────────────────────────────────────────────
        dq_results += [
            check_value_range(con, "lol_player_agg", "win_rate", run_id, lo=0.0, hi=1.0),
            check_value_range(con, "cs_team_agg", "win_rate",    run_id, lo=0.0, hi=1.0),
        ]

        if all_checks_passed(dq_results):
            log.info("✅ All DQ checks passed (%d checks)", len(dq_results))
            status = "success"
        else:
            fail_count = dq_results.count(False)
            log.warning("⚠️  %d/%d DQ checks failed — investigate data_quality_checks table",
                        fail_count, len(dq_results))
            status = "dq_warning"

        _log_run(con, run_id, "transform-flow", status,
                 rows_in=lol_silver_rows + cs_silver_rows,
                 rows_out=lol_silver_rows + cs_silver_rows)

    except Exception as exc:
        _log_run(con, run_id, "transform-flow", "failed", error=str(exc))
        log.exception("❌ Transform flow failed")
        raise
    finally:
        con.close()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main orchestrator — single deployable entrypoint
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@flow(
    name="esports-pipeline-main",
    description="End-to-end run: ingest LoL + CS2 data, then transform Bronze→Silver→Gold with DQ checks.",
    log_prints=True,
)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main orchestrator — single deployable entrypoint
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@flow(
    name="esports-pipeline-main",
    description="End-to-end run: ingest LoL + CS2 data, then transform Bronze→Silver→Gold with DQ checks.",
    log_prints=True,
)
def main(
    summoners: list[dict] | None = None,
    max_per_player: int = 20,
    cs_match_count: int = 100,
):
    """
    Single entrypoint for the whole pipeline. Calls each stage as a subflow,
    ensuring transform_flow only runs if ingestion completes successfully.
    """
    log.info("Starting esports-pipeline-main run")

    # 1. Run ingestion flows and capture their final states
    lol_state = lol_ingest_flow(summoners=summoners, max_per_player=max_per_player, return_state=True)
    cs_state = cs_ingest_flow(count=cs_match_count, return_state=True)

    # 2. Strict dependency check: Only run transforms if both ingests succeeded
    if lol_state.is_completed() and cs_state.is_completed():
        log.info("Ingestion successful. Proceeding to transform_flow.")
        transform_flow()
    else:
        log.error("❌ Transform flow skipped because one or both ingestion flows failed.")
        # Optional: Raise an exception if you want the main Prefect flow to register as a failure
        raise RuntimeError("Pipeline failed during the ingestion stage.")

    log.info("esports-pipeline-main run complete")

    lol_ingest_flow(summoners=summoners, max_per_player=max_per_player)
    cs_ingest_flow(count=cs_match_count)
    transform_flow()

    log.info("esports-pipeline-main run complete")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLI entry-point
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if __name__ == "__main__":
    main()