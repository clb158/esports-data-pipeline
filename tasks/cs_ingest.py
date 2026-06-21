"""
Counter-Strike 2 ingest tasks.

Source: PandaScore API (free tier — 1 000 req/hr, no auth for some endpoints).
Docs:   https://developers.pandascore.co/

Flow
----
  extract_cs_matches  → pull completed pro CS2 matches
  load_raw_cs_matches → upsert raw JSON into raw_cs_matches (bronze)
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import requests
from prefect import task
from prefect.cache_policies import NO_CACHE

from config.settings import (
    PANDASCORE_TOKEN,
    PANDASCORE_BASE,
    CS_MAX_MATCHES,
    REQUEST_TIMEOUT,
)

log = logging.getLogger(__name__)


def _ps_get(endpoint: str, params: Optional[dict] = None) -> list | dict:
    """GET against PandaScore with pagination + retry."""
    url = f"{PANDASCORE_BASE}{endpoint}"
    headers = {}
    if PANDASCORE_TOKEN and PANDASCORE_TOKEN != "demo_token":
        headers["Authorization"] = f"Bearer {PANDASCORE_TOKEN}"

    for attempt in range(3):
        try:
            resp = requests.get(url, headers=headers, params=params,
                                timeout=REQUEST_TIMEOUT)
            if resp.status_code == 429:
                log.warning("PandaScore rate limit — sleeping 60s")
                time.sleep(60)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            log.error("PandaScore request failed (attempt %d): %s", attempt + 1, exc)
            time.sleep(5)

    return []


# ── Tasks ─────────────────────────────────────────────────────────────────

@task(name="extract-cs-matches", retries=2, retry_delay_seconds=60)
def extract_cs_matches(
    count: int = CS_MAX_MATCHES,
    tournament_slug: Optional[str] = None,  # e.g. "cs-go-esl-pro-league-season-20"
    since_id: int = 0,                      # 👈 Add this new parameter
) -> list[dict]:
    """
    Pull the most recent completed CS2 pro matches from PandaScore.
    Filters out matches with an ID less than or equal to since_id.
    """
    params: dict = {
        "page[size]": min(count, 100),   # max page size = 100
        "sort": "id",                    # 👈 CHANGE sorting from "-end_at" to "id" for clean linear tracking
        "filter[status]": "finished",
    }
    
    if tournament_slug:
        params["filter[tournament_slug]"] = tournament_slug
        
    # 👈 Add the incremental filter if we have a valid previous ID
    if since_id > 0:
        params["filter[id][gt]"] = since_id

    endpoint = "/csgo/matches"
    data = _ps_get(endpoint, params=params)

    if not isinstance(data, list):
        log.error("Unexpected response type from PandaScore: %s", type(data))
        return []

    log.info("Fetched %d CS2 matches from PandaScore", len(data))
    return data


@task(name="extract-cs-match-detail", retries=2, retry_delay_seconds=30)
def extract_cs_match_detail(match_id: int) -> Optional[dict]:
    """Fetch detailed stats for a single CS2 match (includes games/rounds)."""
    data = _ps_get(f"/csgo/matches/{match_id}")
    if isinstance(data, dict) and data.get("id"):
        log.debug("Fetched CS2 match detail for %d", match_id)
        return data
    return None


@task(name="load-raw-cs-matches", retries=1, cache_policy=NO_CACHE)
def load_raw_cs_matches(matches: list[dict], con) -> int:
    """
    Upsert raw CS2 match JSON into bronze table raw_cs_matches.

    Returns number of new rows inserted.
    """
    if not matches:
        log.warning("No CS2 matches to load")
        return 0

    inserted = 0
    for match in matches:
        if not match or not match.get("id"):
            continue

        match_id = match["id"]
        tournament = (match.get("tournament") or {}).get("name", "unknown")

        exists = con.execute(
            "SELECT 1 FROM raw_cs_matches WHERE match_id = ?", [match_id]
        ).fetchone()

        if exists:
            log.debug("CS2 match %d already in warehouse — skipping", match_id)
            continue

        con.execute(
            """
            INSERT INTO raw_cs_matches (match_id, tournament_name, raw_json, ingested_at)
            VALUES (?, ?, ?, ?)
            """,
            [match_id, tournament, json.dumps(match), datetime.now(timezone.utc)],
        )
        inserted += 1

    log.info("Loaded %d new CS2 matches into bronze layer", inserted)
    return inserted


@task(name="extract-cs-upcoming", retries=2, retry_delay_seconds=60)
def extract_cs_upcoming(count: int = 20) -> list[dict]:
    """Pull upcoming CS2 matches — useful for a 'next matches' dashboard widget."""
    params = {
        "page[size]": count,
        "sort": "begin_at",
        "filter[status]": "not_started",
    }
    data = _ps_get("/csgo/matches", params=params)
    log.info("Fetched %d upcoming CS2 matches", len(data) if isinstance(data, list) else 0)
    return data if isinstance(data, list) else []
