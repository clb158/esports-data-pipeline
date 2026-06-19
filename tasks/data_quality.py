"""
Data quality tasks — reusable assertions that run after every ingest/transform.

Each check:
  1. Runs a SQL query against DuckDB
  2. Compares metric to threshold
  3. Logs result to data_quality_checks table
  4. Returns bool (True = pass)

Prefect tasks are decorated so they show up in the Prefect UI.
"""

import uuid
import logging
from datetime import datetime, timezone
from typing import Optional

import duckdb
from prefect import task
from prefect.cache_policies import NO_CACHE

log = logging.getLogger(__name__)


def _log_check(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    table: str,
    check_name: str,
    passed: bool,
    metric: float,
    threshold: float,
) -> None:
    con.execute(
        """
        INSERT OR REPLACE INTO data_quality_checks
            (check_id, run_id, table_name, check_name, passed, metric_value, threshold, checked_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [str(uuid.uuid4()), run_id, table, check_name, passed, metric, threshold,
         datetime.now(timezone.utc)],
    )


@task(name="dq-row-count", retries=1, cache_policy=NO_CACHE)
def check_row_count(
    con: duckdb.DuckDBPyConnection,
    table: str,
    run_id: str,
    minimum: int = 1,
) -> bool:
    """Fail if table has fewer than *minimum* rows."""
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    passed = count >= minimum
    _log_check(con, run_id, table, "row_count", passed, float(count), float(minimum))
    status = "✅ PASS" if passed else "❌ FAIL"
    log.info("%s row_count on %s: %d (min=%d)", status, table, count, minimum)
    return passed


@task(name="dq-null-rate", retries=1, cache_policy=NO_CACHE)
def check_null_rate(
    con: duckdb.DuckDBPyConnection,
    table: str,
    column: str,
    run_id: str,
    threshold: float = 0.10,
) -> bool:
    """Fail if null rate on *column* exceeds *threshold*."""
    result = con.execute(
        f"""
        SELECT
            COUNT(*) FILTER (WHERE {column} IS NULL)::DOUBLE / NULLIF(COUNT(*), 0)
        FROM {table}
        """
    ).fetchone()[0]
    null_rate = result or 0.0
    passed = null_rate <= threshold
    _log_check(con, run_id, table, f"null_rate_{column}", passed, null_rate, threshold)
    status = "✅ PASS" if passed else "❌ FAIL"
    log.info("%s null_rate(%s.%s): %.2f%% (max=%.2f%%)",
             status, table, column, null_rate * 100, threshold * 100)
    return passed


@task(name="dq-duplicate-pk", retries=1, cache_policy=NO_CACHE)
def check_no_duplicates(
    con: duckdb.DuckDBPyConnection,
    table: str,
    pk_column: str,
    run_id: str,
) -> bool:
    """Fail if any primary key value appears more than once."""
    dup_count = con.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT {pk_column} FROM {table}
            GROUP BY {pk_column} HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    passed = dup_count == 0
    _log_check(con, run_id, table, f"no_duplicates_{pk_column}", passed,
               float(dup_count), 0.0)
    status = "✅ PASS" if passed else "❌ FAIL"
    log.info("%s no_duplicates(%s.%s): %d dups found", status, table, pk_column, dup_count)
    return passed


@task(name="dq-value-range", retries=1, cache_policy=NO_CACHE)
def check_value_range(
    con: duckdb.DuckDBPyConnection,
    table: str,
    column: str,
    run_id: str,
    lo: Optional[float] = None,
    hi: Optional[float] = None,
) -> bool:
    """Fail if any value in *column* is outside [lo, hi]."""
    where_clauses = []
    if lo is not None:
        where_clauses.append(f"{column} < {lo}")
    if hi is not None:
        where_clauses.append(f"{column} > {hi}")

    if not where_clauses:
        log.warning("check_value_range called with no bounds — skipping")
        return True

    where = " OR ".join(where_clauses)
    out_of_range = con.execute(
        f"SELECT COUNT(*) FROM {table} WHERE {where}"
    ).fetchone()[0]
    passed = out_of_range == 0
    _log_check(con, run_id, table, f"range_{column}", passed,
               float(out_of_range), 0.0)
    status = "✅ PASS" if passed else "❌ FAIL"
    log.info("%s value_range(%s.%s) [%s, %s]: %d violations",
             status, table, column, lo, hi, out_of_range)
    return passed


@task(name="dq-freshness", retries=1, cache_policy=NO_CACHE)
def check_freshness(
    con: duckdb.DuckDBPyConnection,
    table: str,
    ts_column: str,
    run_id: str,
    max_age_hours: float = 24.0,
) -> bool:
    """Fail if the most recent row is older than *max_age_hours*."""
    latest = con.execute(
        f"SELECT MAX({ts_column}) FROM {table}"
    ).fetchone()[0]

    if latest is None:
        log.warning("freshness check skipped — table %s is empty", table)
        return True  # row_count check will catch empty tables

    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    age_hours = (now - latest).total_seconds() / 3600
    passed = age_hours <= max_age_hours
    _log_check(con, run_id, table, f"freshness_{ts_column}", passed,
               age_hours, max_age_hours)
    status = "✅ PASS" if passed else "❌ FAIL"
    log.info("%s freshness(%s.%s): %.1f hrs old (max=%.1f)", 
             status, table, ts_column, age_hours, max_age_hours)
    return passed


def all_checks_passed(results: list[bool]) -> bool:
    """Utility: return True only if every DQ check passed."""
    return all(results)
