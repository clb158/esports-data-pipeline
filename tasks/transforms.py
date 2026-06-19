"""
Transform tasks — Bronze → Silver → Gold.

Bronze  : raw JSON stored as strings
Silver  : normalised relational rows
Gold    : aggregated KPI tables (the ones analysts query)

All transforms are idempotent — safe to re-run without double-counting.
"""

import json
import logging
import uuid
from datetime import datetime, timezone

from prefect import task
from prefect.cache_policies import NO_CACHE

log = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# League of Legends — Bronze → Silver
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _parse_lol_match(raw_json: str) -> list[dict]:
    """Parse one raw LoL match JSON into per-participant row dicts."""
    try:
        match = json.loads(raw_json)
    except json.JSONDecodeError:
        log.error("Failed to parse LoL match JSON")
        return []

    meta = match.get("metadata", {})
    info = match.get("info", {})
    match_id = meta.get("matchId", "")
    game_duration = info.get("gameDuration", 0)
    game_version  = info.get("gameVersion", "")
    game_start_ts = info.get("gameStartTimestamp", 0)
    match_ts = datetime.fromtimestamp(game_start_ts / 1000, tz=timezone.utc) \
        if game_start_ts else None

    rows = []
    for p in info.get("participants", []):
        kills   = p.get("kills", 0)
        deaths  = p.get("deaths", 0)
        assists = p.get("assists", 0)

        # KDA: (K + A) / max(D, 1) — industry standard
        kda = round((kills + assists) / max(deaths, 1), 2)

        # CS per minute
        cs_total  = p.get("totalMinionsKilled", 0) + p.get("neutralMinionsKilled", 0)
        cs_per_min = round(cs_total / (game_duration / 60), 2) if game_duration > 0 else 0.0

        row = {
            "row_id":         f"{match_id}_{p.get('puuid', '')}",
            "match_id":       match_id,
            "puuid":          p.get("puuid", ""),
            "summoner_name":  p.get("riotIdGameName", p.get("summonerName", "")),
            "champion_name":  p.get("championName", ""),
            "team_position":  p.get("teamPosition", ""),
            "win":            p.get("win", False),
            "kills":          kills,
            "deaths":         deaths,
            "assists":        assists,
            "kda":            kda,
            "cs_per_min":     cs_per_min,
            "vision_score":   p.get("visionScore", 0),
            "damage_dealt":   p.get("totalDamageDealtToChampions", 0),
            "gold_earned":    p.get("goldEarned", 0),
            "match_duration": game_duration,
            "game_version":   game_version,
            "match_ts":       match_ts,
            "processed_at":   datetime.now(timezone.utc),
        }
        rows.append(row)
    return rows


@task(name="transform-lol-silver", retries=1, cache_policy=NO_CACHE)
def transform_lol_silver(con) -> int:
    """
    Read all unprocessed rows from raw_lol_matches,
    expand to per-participant rows in lol_match_stats.
    Returns number of new silver rows written.
    """
    # Find match IDs not yet in silver
    unprocessed = con.execute("""
        SELECT r.match_id, r.raw_json
        FROM raw_lol_matches r
        LEFT JOIN (
            SELECT DISTINCT match_id FROM lol_match_stats
        ) s ON r.match_id = s.match_id
        WHERE s.match_id IS NULL
    """).fetchall()

    if not unprocessed:
        log.info("No new LoL matches to transform (silver is up-to-date)")
        return 0

    inserted = 0
    for match_id, raw_json in unprocessed:
        rows = _parse_lol_match(raw_json)
        for row in rows:
            try:
                con.execute("""
                    INSERT OR REPLACE INTO lol_match_stats VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                """, [
                    row["row_id"], row["match_id"], row["puuid"], row["summoner_name"],
                    row["champion_name"], row["team_position"], row["win"],
                    row["kills"], row["deaths"], row["assists"], row["kda"],
                    row["cs_per_min"], row["vision_score"], row["damage_dealt"],
                    row["gold_earned"], row["match_duration"], row["game_version"],
                    row["match_ts"], row["processed_at"],
                ])
                inserted += 1
            except Exception as exc:
                log.error("Failed to insert silver row %s: %s", row["row_id"], exc)

    log.info("LoL silver transform complete — %d rows written", inserted)
    return inserted


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Counter-Strike — Bronze → Silver
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _parse_cs_match(raw_json: str) -> list[dict]:
    """Parse one raw CS2 match JSON into per-team row dicts."""
    try:
        match = json.loads(raw_json)
    except json.JSONDecodeError:
        log.error("Failed to parse CS2 match JSON")
        return []

    match_id = match.get("id")
    tournament = (match.get("tournament") or {}).get("name", "")
    begin_at_str = match.get("end_at") or match.get("begin_at")
    match_ts = None
    if begin_at_str:
        try:
            match_ts = datetime.fromisoformat(begin_at_str.replace("Z", "+00:00"))
        except ValueError:
            pass

    opponents = match.get("opponents", [])
    results   = match.get("results", [])

    # Build lookup: team_id → score
    result_map = {r.get("team_id"): r.get("score", 0) for r in results if r.get("team_id")}
    winner_id  = (match.get("winner") or {}).get("id")

    rows = []
    for opp_wrapper in opponents:
        opp = opp_wrapper.get("opponent", {})
        team_id   = opp.get("id")
        team_name = opp.get("name", "")
        if not team_id:
            continue

        my_score  = result_map.get(team_id, 0)
        opp_score = sum(v for k, v in result_map.items() if k != team_id)

        rows.append({
            "row_id":          f"{match_id}_{team_id}",
            "match_id":        match_id,
            "team_id":         team_id,
            "team_name":       team_name,
            "opponent_name":   "",           # filled in post-loop
            "win":             (team_id == winner_id),
            "rounds_won":      my_score,
            "rounds_lost":     opp_score,
            "score":           f"{my_score}-{opp_score}",
            "tournament_name": tournament,
            "match_ts":        match_ts,
            "processed_at":    datetime.now(timezone.utc),
        })

    # Back-fill opponent names
    if len(rows) == 2:
        rows[0]["opponent_name"] = rows[1]["team_name"]
        rows[1]["opponent_name"] = rows[0]["team_name"]

    return rows


@task(name="transform-cs-silver", retries=1, cache_policy=NO_CACHE)
def transform_cs_silver(con) -> int:
    """Bronze → silver for CS2 matches."""
    unprocessed = con.execute("""
        SELECT r.match_id, r.raw_json
        FROM raw_cs_matches r
        LEFT JOIN (
            SELECT DISTINCT match_id FROM cs_match_stats
        ) s ON r.match_id = s.match_id
        WHERE s.match_id IS NULL
    """).fetchall()

    if not unprocessed:
        log.info("No new CS2 matches to transform (silver is up-to-date)")
        return 0

    inserted = 0
    for match_id, raw_json in unprocessed:
        rows = _parse_cs_match(raw_json)
        for row in rows:
            try:
                con.execute("""
                    INSERT OR REPLACE INTO cs_match_stats VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                """, [
                    row["row_id"], row["match_id"], row["team_id"], row["team_name"],
                    row["opponent_name"], row["win"], row["rounds_won"], row["rounds_lost"],
                    row["score"], row["tournament_name"], row["match_ts"], row["processed_at"],
                ])
                inserted += 1
            except Exception as exc:
                log.error("Failed to insert CS2 silver row %s: %s", row["row_id"], exc)

    log.info("CS2 silver transform complete — %d rows written", inserted)
    return inserted


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Gold aggregations
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@task(name="aggregate-lol-gold", retries=1, cache_policy=NO_CACHE)
def aggregate_lol_gold(con) -> int:
    """
    Rebuild lol_player_agg from lol_match_stats.
    Gold layer is fully replaced on each run (small table, fast).
    """
    con.execute("DELETE FROM lol_player_agg")
    con.execute("""
        INSERT INTO lol_player_agg
        SELECT
            puuid,
            ANY_VALUE(summoner_name)                        AS summoner_name,
            COUNT(*)                                        AS games_played,
            ROUND(AVG(win::INTEGER)::DOUBLE, 4)            AS win_rate,
            ROUND(AVG(kda), 2)                              AS avg_kda,
            ROUND(AVG(kills), 2)                            AS avg_kills,
            ROUND(AVG(deaths), 2)                           AS avg_deaths,
            ROUND(AVG(assists), 2)                          AS avg_assists,
            ROUND(AVG(cs_per_min), 2)                       AS avg_cs_per_min,
            ROUND(AVG(vision_score), 1)                     AS avg_vision,
            MODE(champion_name)                             AS top_champion,
            NOW()                                           AS updated_at
        FROM lol_match_stats
        WHERE puuid IS NOT NULL AND puuid != ''
        GROUP BY puuid
    """)
    count = con.execute("SELECT COUNT(*) FROM lol_player_agg").fetchone()[0]
    log.info("LoL gold aggregate rebuilt — %d player rows", count)
    return count


@task(name="aggregate-cs-gold", retries=1, cache_policy=NO_CACHE)
def aggregate_cs_gold(con) -> int:
    """Rebuild cs_team_agg from cs_match_stats."""
    con.execute("DELETE FROM cs_team_agg")
    con.execute("""
        INSERT INTO cs_team_agg
        SELECT
            team_id,
            ANY_VALUE(team_name)                           AS team_name,
            COUNT(*)                                       AS matches_played,
            ROUND(AVG(win::INTEGER)::DOUBLE, 4)           AS win_rate,
            ROUND(AVG(rounds_won), 2)                      AS avg_rounds_won,
            ROUND(AVG(rounds_lost), 2)                     AS avg_rounds_lost,
            NOW()                                          AS updated_at
        FROM cs_match_stats
        WHERE team_id IS NOT NULL
        GROUP BY team_id
    """)
    count = con.execute("SELECT COUNT(*) FROM cs_team_agg").fetchone()[0]
    log.info("CS2 gold aggregate rebuilt — %d team rows", count)
    return count
