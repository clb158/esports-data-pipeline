"""
Test suite for the VG analytics pipeline.

Tests are isolated — each uses an in-memory DuckDB instance,
so they never touch the real warehouse file.

Run with:
    cd /home/claude/vgpipeline
    python -m pytest tests/ -v
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

# Make project root importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from warehouse.schema import DDL
from tasks.transforms import _parse_lol_match, _parse_cs_match
from tasks.data_quality import (
    check_row_count,
    check_null_rate,
    check_no_duplicates,
    check_value_range,
    all_checks_passed,
)


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def memdb():
    """Fresh in-memory DuckDB with full schema applied."""
    con = duckdb.connect(":memory:")
    con.execute(DDL)
    yield con
    con.close()


SAMPLE_LOL_MATCH = {
    "metadata": {"matchId": "NA1_1234567890"},
    "info": {
        "gameDuration": 1800,
        "gameVersion": "14.10.1",
        "gameStartTimestamp": 1700000000000,
        "participants": [
            {
                "puuid": "puuid-abc",
                "riotIdGameName": "TestPlayer",
                "championName": "Jinx",
                "teamPosition": "BOTTOM",
                "win": True,
                "kills": 8,
                "deaths": 2,
                "assists": 5,
                "totalMinionsKilled": 180,
                "neutralMinionsKilled": 20,
                "visionScore": 25,
                "totalDamageDealtToChampions": 35000,
                "goldEarned": 14000,
            }
        ],
    },
}

SAMPLE_CS_MATCH = {
    "id": 999001,
    "end_at": "2024-01-15T20:00:00Z",
    "tournament": {"name": "ESL Pro League Season 20"},
    "winner": {"id": 1},
    "opponents": [
        {"opponent": {"id": 1, "name": "Team Vitality"}},
        {"opponent": {"id": 2, "name": "FaZe Clan"}},
    ],
    "results": [
        {"team_id": 1, "score": 16},
        {"team_id": 2, "score": 14},
    ],
}


# ── Transform tests ────────────────────────────────────────────────────────

class TestLoLParser:
    def test_basic_parsing(self):
        rows = _parse_lol_match(json.dumps(SAMPLE_LOL_MATCH))
        assert len(rows) == 1, "Should produce one row per participant"

    def test_kda_calculation(self):
        rows = _parse_lol_match(json.dumps(SAMPLE_LOL_MATCH))
        row = rows[0]
        # (8 + 5) / max(2, 1) = 6.5
        assert row["kda"] == 6.5

    def test_cs_per_min(self):
        rows = _parse_lol_match(json.dumps(SAMPLE_LOL_MATCH))
        row = rows[0]
        # (180 + 20) / (1800 / 60) = 200 / 30 ≈ 6.67
        assert abs(row["cs_per_min"] - 6.67) < 0.01

    def test_row_id_format(self):
        rows = _parse_lol_match(json.dumps(SAMPLE_LOL_MATCH))
        assert rows[0]["row_id"] == "NA1_1234567890_puuid-abc"

    def test_bad_json_returns_empty(self):
        rows = _parse_lol_match("not valid json{{{")
        assert rows == []

    def test_zero_duration_cs_per_min(self):
        match = json.loads(json.dumps(SAMPLE_LOL_MATCH))
        match["info"]["gameDuration"] = 0
        rows = _parse_lol_match(json.dumps(match))
        assert rows[0]["cs_per_min"] == 0.0

    def test_zero_deaths_kda(self):
        match = json.loads(json.dumps(SAMPLE_LOL_MATCH))
        match["info"]["participants"][0]["deaths"] = 0
        rows = _parse_lol_match(json.dumps(match))
        # (8 + 5) / max(0, 1) = 13.0
        assert rows[0]["kda"] == 13.0


class TestCSParser:
    def test_basic_parsing(self):
        rows = _parse_cs_match(json.dumps(SAMPLE_CS_MATCH))
        assert len(rows) == 2, "Should produce one row per team"

    def test_win_flag(self):
        rows = _parse_cs_match(json.dumps(SAMPLE_CS_MATCH))
        by_team = {r["team_id"]: r for r in rows}
        assert by_team[1]["win"] is True
        assert by_team[2]["win"] is False

    def test_opponent_names_filled(self):
        rows = _parse_cs_match(json.dumps(SAMPLE_CS_MATCH))
        by_team = {r["team_id"]: r for r in rows}
        assert by_team[1]["opponent_name"] == "FaZe Clan"
        assert by_team[2]["opponent_name"] == "Team Vitality"

    def test_score_string(self):
        rows = _parse_cs_match(json.dumps(SAMPLE_CS_MATCH))
        by_team = {r["team_id"]: r for r in rows}
        assert by_team[1]["score"] == "16-14"
        assert by_team[2]["score"] == "14-16"

    def test_bad_json_returns_empty(self):
        rows = _parse_cs_match("{{bad")
        assert rows == []


# ── Data quality tests ─────────────────────────────────────────────────────

class TestDataQuality:
    RUN_ID = "test-run-001"

    def _seed_lol(self, con, n=5):
        """Insert n synthetic lol_match_stats rows."""
        for i in range(n):
            con.execute("""
                INSERT INTO lol_match_stats
                (row_id, match_id, puuid, summoner_name, champion_name, team_position,
                 win, kills, deaths, assists, kda, cs_per_min, vision_score,
                 damage_dealt, gold_earned, match_duration, game_version, match_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                f"row_{i}", f"match_{i}", f"puuid_{i}", f"Player{i}",
                "Jinx", "BOTTOM", i % 2 == 0,
                5, 2, 8, 6.5, 7.0, 20, 30000, 12000, 1800, "14.10", None
            ])

    def test_row_count_pass(self, memdb):
        self._seed_lol(memdb, 5)
        result = check_row_count.fn(memdb, "lol_match_stats", self.RUN_ID, minimum=1)
        assert result is True

    def test_row_count_fail(self, memdb):
        result = check_row_count.fn(memdb, "lol_match_stats", self.RUN_ID, minimum=1)
        assert result is False

    def test_null_rate_pass(self, memdb):
        self._seed_lol(memdb, 5)
        result = check_null_rate.fn(memdb, "lol_match_stats", "puuid", self.RUN_ID, 0.10)
        assert result is True

    def test_null_rate_fail(self, memdb):
        # Insert rows with null puuid
        for i in range(5):
            memdb.execute("""
                INSERT INTO lol_match_stats
                (row_id, match_id, puuid, summoner_name, champion_name, team_position,
                 win, kills, deaths, assists, kda, cs_per_min, vision_score,
                 damage_dealt, gold_earned, match_duration, game_version)
                VALUES (?, ?, NULL, '', '', '', false, 0, 0, 0, 0, 0, 0, 0, 0, 0, '')
            """, [f"null_row_{i}", f"match_{i}"])
        result = check_null_rate.fn(memdb, "lol_match_stats", "puuid", self.RUN_ID, 0.10)
        assert result is False

    def test_no_duplicates_pass(self, memdb):
        self._seed_lol(memdb, 5)
        result = check_no_duplicates.fn(memdb, "lol_match_stats", "row_id", self.RUN_ID)
        assert result is True

    def test_value_range_pass(self, memdb):
        self._seed_lol(memdb, 5)
        result = check_value_range.fn(memdb, "lol_match_stats", "kda", self.RUN_ID, lo=0.0)
        assert result is True

    def test_all_checks_passed_utility(self):
        assert all_checks_passed([True, True, True]) is True
        assert all_checks_passed([True, False, True]) is False
        assert all_checks_passed([]) is True

    def test_dq_results_persisted(self, memdb):
        self._seed_lol(memdb, 3)
        check_row_count.fn(memdb, "lol_match_stats", self.RUN_ID, minimum=1)
        rows = memdb.execute("SELECT COUNT(*) FROM data_quality_checks").fetchone()[0]
        assert rows >= 1, "DQ checks should be persisted to data_quality_checks table"
