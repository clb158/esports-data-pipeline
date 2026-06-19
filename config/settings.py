"""
Pipeline configuration — all knobs in one place.
Swap in real API keys via environment variables.
"""
import os
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
WAREHOUSE_DIR = BASE_DIR / "warehouse"
WAREHOUSE_DIR.mkdir(exist_ok=True)

DB_PATH = str(WAREHOUSE_DIR / "vg_analytics.duckdb")

# ── APIs ───────────────────────────────────────────────────────────────────
# League of Legends (Riot Games)
RIOT_API_KEY = os.getenv("RIOT_API_KEY", "RGAPI-demo-key")
RIOT_REGION   = os.getenv("RIOT_REGION", "na1")          # na1 | euw1 | kr …
RIOT_BASE_URL = f"https://{RIOT_REGION}.api.riotgames.com"
RIOT_MATCH_URL = "https://americas.api.riotgames.com"    # routing for match-v5

# Counter-Strike 2 (PandaScore — free tier, 1 000 req/hr)
PANDASCORE_TOKEN = os.getenv("PANDASCORE_TOKEN", "demo_token")
PANDASCORE_BASE  = "https://api.pandascore.co"

# ── Scheduling ─────────────────────────────────────────────────────────────
LOL_INGEST_CRON   = "0 */6 * * *"    # every 6 hours
CS_INGEST_CRON    = "30 */6 * * *"   # offset by 30 min
TRANSFORM_CRON    = "0 */12 * * *"   # every 12 hours
DASHBOARD_CRON    = "0 8 * * *"      # daily at 08:00

# ── Ingest limits (keep within free-tier rate limits) ──────────────────────
LOL_MAX_MATCHES   = 50   # matches pulled per run
CS_MAX_MATCHES    = 100  # pro matches pulled per run
REQUEST_TIMEOUT   = 10   # seconds

# ── Data quality thresholds ────────────────────────────────────────────────
NULL_THRESHOLD    = 0.10   # fail if > 10 % nulls on critical column
ROW_MIN           = 1      # fail if table is empty after ingest

# ── Logging ────────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
