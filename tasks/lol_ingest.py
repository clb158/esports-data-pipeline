"""
League of Legends ingest tasks.

Flow
----
  extract_lol_summoner   → look up PUUID from summoner name
  extract_lol_match_ids  → pull recent match IDs for that PUUID
  extract_lol_match      → fetch full match JSON for one match ID
  load_raw_lol_matches   → upsert raw JSON rows into raw_lol_matches (bronze)

Uses Riot Games API (rate-limited: 20 req/sec, 100 req/2min on dev key).
"""

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import requests
from prefect import task

from config.settings import (
    RIOT_API_KEY,
    RIOT_BASE_URL,
    RIOT_MATCH_URL,
    RIOT_REGION,
    LOL_MAX_MATCHES,
    REQUEST_TIMEOUT,
)

log = logging.getLogger(__name__)

HEADERS = {"X-Riot-Token": RIOT_API_KEY}


# ── Helpers ───────────────────────────────────────────────────────────────

def _get(url: str, params: Optional[dict] = None) -> dict:
    """GET with retry logic for Riot 429s."""
    for attempt in range(3):
        resp = requests.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 5))
            log.warning("Rate limited — sleeping %ds (attempt %d/3)", retry_after, attempt + 1)
            time.sleep(retry_after)
            continue
        if resp.status_code == 403:
            log.error(
                "Riot API key invalid or expired. Set RIOT_API_KEY env var. "
                "Get a dev key at https://developer.riotgames.com/"
            )
            return {}
        resp.raise_for_status()
        return resp.json()
    return {}


# ── Tasks ─────────────────────────────────────────────────────────────────

@task(name="extract-lol-summoner", retries=2, retry_delay_seconds=30)
def extract_lol_summoner(summoner_name: str, tag_line: str = "NA1") -> Optional[str]:
    """
    Resolve summoner name + tag → PUUID via Riot Account API.

    Returns PUUID string, or None if not found.
    """
    url = f"https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{summoner_name}/{tag_line}"
    data = _get(url)
    puuid = data.get("puuid")
    if puuid:
        log.info("Resolved %s#%s → %s", summoner_name, tag_line, puuid[:8] + "…")
    else:
        log.warning("Could not resolve summoner %s#%s", summoner_name, tag_line)
    return puuid


@task(name="extract-lol-match-ids", retries=2, retry_delay_seconds=30)
def extract_lol_match_ids(
    puuid: str,
    count: int = LOL_MAX_MATCHES,
    queue_id: int = 420,         # 420 = Solo/Duo Ranked
) -> list[str]:
    """
    Pull the last *count* ranked match IDs for *puuid*.

    queue_id options:
      420 = Solo/Duo Ranked | 440 = Flex | 450 = ARAM | 400 = Normal Draft
    """
    url = f"{RIOT_MATCH_URL}/lol/match/v5/matches/by-puuid/{puuid}/ids"
    params = {"queue": queue_id, "count": count}
    ids = _get(url, params=params)
    if isinstance(ids, list):
        log.info("Fetched %d match IDs for puuid %s…", len(ids), puuid[:8])
        return ids
    return []


@task(name="extract-lol-match", retries=2, retry_delay_seconds=15)
def extract_lol_match(match_id: str) -> Optional[dict]:
    """Fetch full match JSON for a single match ID."""
    url = f"{RIOT_MATCH_URL}/lol/match/v5/matches/{match_id}"
    data = _get(url)
    if data:
        log.debug("Fetched match %s", match_id)
    else:
        log.warning("No data returned for match %s", match_id)
    return data or None


@task(name="load-raw-lol-matches", retries=1)
def load_raw_lol_matches(matches: list[dict], con) -> int:
    """
    Upsert raw match JSON into bronze table raw_lol_matches.

    Returns number of new rows inserted.
    """
    if not matches:
        log.warning("No matches to load")
        return 0

    inserted = 0
    for match in matches:
        if not match:
            continue
        match_id = match.get("metadata", {}).get("matchId", "")
        if not match_id:
            log.warning("Skipping match with no matchId")
            continue

        # Check for existing row (idempotent upsert)
        exists = con.execute(
            "SELECT 1 FROM raw_lol_matches WHERE match_id = ?", [match_id]
        ).fetchone()

        if exists:
            log.debug("Match %s already in warehouse — skipping", match_id)
            continue

        con.execute(
            """
            INSERT INTO raw_lol_matches (match_id, region, raw_json, ingested_at)
            VALUES (?, ?, ?, ?)
            """,
            [match_id, RIOT_REGION, json.dumps(match), datetime.now(timezone.utc)],
        )
        inserted += 1

    log.info("Loaded %d new raw LoL matches into bronze layer", inserted)
    return inserted


@task(name="extract-lol-batch", retries=1)
def extract_lol_batch(
    summoners: list[dict],   # [{"name": "Faker", "tag": "T1"}, …]
    max_matches_each: int = 20,
) -> list[dict]:
    """
    Convenience task: given a list of summoner dicts, pull all their recent
    matches and return a deduplicated flat list of match JSON objects.
    """
    seen_ids: set[str] = set()
    all_matches: list[dict] = []

    for s in summoners:
        name = s.get("name", "")
        tag  = s.get("tag", "NA1")

        puuid = extract_lol_summoner.fn(name, tag)
        if not puuid:
            continue

        ids = extract_lol_match_ids.fn(puuid, count=max_matches_each)
        for mid in ids:
            if mid in seen_ids:
                continue
            seen_ids.add(mid)
            time.sleep(0.05)  # 50 ms between requests ≈ 20 req/sec
            match = extract_lol_match.fn(mid)
            if match:
                all_matches.append(match)

    log.info("Batch ingest complete — %d unique matches fetched", len(all_matches))
    return all_matches
