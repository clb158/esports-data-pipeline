"""
demo_seed.py — populate the warehouse with synthetic data so you can explore
the pipeline and dashboard without real API keys.

Run:
    cd /home/claude/vgpipeline
    python demo_seed.py
"""

import json
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from warehouse.schema import init_warehouse, get_connection
from tasks.transforms import transform_lol_silver, transform_cs_silver
from tasks.transforms import aggregate_lol_gold, aggregate_cs_gold

# ── Seed constants ─────────────────────────────────────────────────────────
CHAMPIONS = [
    "Jinx", "Lux", "Thresh", "Zed", "Ahri", "Yasuo", "Caitlyn",
    "Leona", "Alistar", "Malphite", "Vi", "Ekko", "Cassiopeia",
]
POSITIONS = ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]
SUMMONERS = [
    ("Faker",      "T1",   "puuid-faker"),
    ("Caps",       "EUW",  "puuid-caps"),
    ("Doublelift", "100",  "puuid-dbl"),
    ("Ruler",      "KR",   "puuid-ruler"),
    ("Jankos",     "G2",   "puuid-jankos"),
]

CS_TEAMS = [
    (101, "Team Vitality"),
    (102, "FaZe Clan"),
    (103, "Natus Vincere"),
    (104, "MOUZ"),
    (105, "G2 Esports"),
]

TOURNAMENTS = [
    "ESL Pro League Season 20",
    "BLAST Premier World Final",
    "IEM Katowice 2024",
]

rng = random.Random(42)


def _lol_participant(puuid: str, name: str, win: bool) -> dict:
    kills   = rng.randint(2, 18)
    deaths  = rng.randint(1, 10)
    assists = rng.randint(3, 20)
    cs      = rng.randint(120, 280)
    return {
        "puuid":                        puuid,
        "riotIdGameName":               name,
        "championName":                 rng.choice(CHAMPIONS),
        "teamPosition":                 rng.choice(POSITIONS),
        "win":                          win,
        "kills":                        kills,
        "deaths":                       deaths,
        "assists":                      assists,
        "totalMinionsKilled":           cs,
        "neutralMinionsKilled":         rng.randint(0, 40),
        "visionScore":                  rng.randint(10, 60),
        "totalDamageDealtToChampions":  rng.randint(10_000, 60_000),
        "goldEarned":                   rng.randint(8_000, 18_000),
    }


def seed_lol_matches(con, n: int = 60):
    print(f"Seeding {n} synthetic LoL matches…")
    now = datetime.now(timezone.utc)
    inserted = 0

    for i in range(n):
        match_id = f"NA1_SEED_{i:06d}"
        game_duration = rng.randint(1200, 2400)
        game_start_ts = int(
            (now - timedelta(days=rng.randint(0, 30))).timestamp() * 1000
        )

        # Pick 5 random summoners for team 1, fill rest with bots
        participants = []
        for j, (name, tag, puuid) in enumerate(SUMMONERS):
            win = j < 3  # first 3 summoners are on winning team
            participants.append(_lol_participant(puuid, name, win))

        # Pad to 10 participants with synthetic players
        for k in range(10 - len(participants)):
            synthetic_puuid = f"puuid-bot-{i}-{k}"
            participants.append(
                _lol_participant(synthetic_puuid, f"Bot{k}", k < 5)
            )

        match = {
            "metadata": {"matchId": match_id},
            "info": {
                "gameDuration":        game_duration,
                "gameVersion":         f"14.{rng.randint(1, 15)}.1",
                "gameStartTimestamp":  game_start_ts,
                "participants":        participants,
            },
        }

        con.execute(
            "INSERT OR IGNORE INTO raw_lol_matches (match_id, region, raw_json, ingested_at) "
            "VALUES (?, ?, ?, ?)",
            [match_id, "na1", json.dumps(match), now],
        )
        inserted += 1

    print(f"  → {inserted} raw LoL matches written to bronze layer")


def seed_cs_matches(con, n: int = 80):
    print(f"Seeding {n} synthetic CS2 matches…")
    now = datetime.now(timezone.utc)
    inserted = 0

    for i in range(n):
        match_id = 800_000 + i
        t1, t2 = rng.sample(CS_TEAMS, 2)

        t1_rounds = rng.randint(9, 16)
        t2_rounds = 16 - t1_rounds if t1_rounds < 16 else rng.randint(9, 15)
        winner_id = t1[0] if t1_rounds > t2_rounds else t2[0]
        end_at = (now - timedelta(days=rng.randint(0, 60))).isoformat()

        match = {
            "id":         match_id,
            "end_at":     end_at,
            "tournament": {"name": rng.choice(TOURNAMENTS)},
            "winner":     {"id": winner_id},
            "opponents": [
                {"opponent": {"id": t1[0], "name": t1[1]}},
                {"opponent": {"id": t2[0], "name": t2[1]}},
            ],
            "results": [
                {"team_id": t1[0], "score": t1_rounds},
                {"team_id": t2[0], "score": t2_rounds},
            ],
        }

        con.execute(
            "INSERT OR IGNORE INTO raw_cs_matches (match_id, tournament_name, raw_json, ingested_at) "
            "VALUES (?, ?, ?, ?)",
            [match_id, rng.choice(TOURNAMENTS), json.dumps(match), now],
        )
        inserted += 1

    print(f"  → {inserted} raw CS2 matches written to bronze layer")


def run_transforms(con):
    print("Running bronze → silver → gold transforms…")
    lol_rows = transform_lol_silver.fn(con)
    cs_rows  = transform_cs_silver.fn(con)
    lol_gold = aggregate_lol_gold.fn(con)
    cs_gold  = aggregate_cs_gold.fn(con)
    print(f"  LoL silver: {lol_rows} rows  |  CS2 silver: {cs_rows} rows")
    print(f"  LoL gold: {lol_gold} players |  CS2 gold: {cs_gold} teams")


def print_summary(con):
    print("\n── Warehouse summary ─────────────────────────────────────────")
    for table in [
        "raw_lol_matches", "raw_cs_matches",
        "lol_match_stats", "cs_match_stats",
        "lol_player_agg", "cs_team_agg",
    ]:
        n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table:<25} {n:>5} rows")

    print("\n── Top LoL players by avg KDA ────────────────────────────────")
    rows = con.execute("""
        SELECT summoner_name, games_played, ROUND(avg_kda,2) AS kda,
               ROUND(win_rate*100,1) AS wr_pct, top_champion
        FROM lol_player_agg
        ORDER BY avg_kda DESC LIMIT 5
    """).fetchall()
    print(f"  {'Name':<15} {'GP':>4} {'KDA':>6} {'WR%':>6} {'Top Champ'}")
    for r in rows:
        print(f"  {r[0]:<15} {r[1]:>4} {r[2]:>6} {r[3]:>6}  {r[4]}")

    print("\n── CS2 team win rates ────────────────────────────────────────")
    rows = con.execute("""
        SELECT team_name, matches_played, ROUND(win_rate*100,1) AS wr_pct,
               ROUND(avg_rounds_won,1) AS avg_rw
        FROM cs_team_agg
        ORDER BY win_rate DESC
    """).fetchall()
    print(f"  {'Team':<20} {'MP':>4} {'WR%':>6} {'Avg RW':>7}")
    for r in rows:
        print(f"  {r[0]:<20} {r[1]:>4} {r[2]:>6}  {r[3]:>6}")
    print()


if __name__ == "__main__":
    print("VG Analytics Pipeline — Demo Seed\n")
    init_warehouse()
    con = get_connection()

    seed_lol_matches(con)
    seed_cs_matches(con)
    run_transforms(con)
    print_summary(con)

    con.close()
    print("✅ Seed complete. Run 'python dashboard/app.py' to launch the dashboard.")
