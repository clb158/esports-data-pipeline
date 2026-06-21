"""
Esports Data Pipeline — Streamlit Dashboard

Reads directly from the DuckDB warehouse populated by the Prefect pipeline.
No write access — this is a read-only view layer.

Run locally:
    streamlit run dashboard/app.py

Deploy:
    Push to GitHub, then deploy via share.streamlit.io pointed at this file.
    Note: Streamlit Cloud needs its own copy of the .duckdb file or a
    MotherDuck connection string — see README for cloud deployment notes.
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import DB_PATH

# ── Page config ──────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Esports Analytics Pipeline",
    page_icon="🎮",
    layout="wide",
)

# ── Data access layer ────────────────────────────────────────────────────

@st.cache_resource
def get_connection():
    """Open a read-only connection to the warehouse."""
    return duckdb.connect(DB_PATH, read_only=True)


@st.cache_data(ttl=300)  # refresh every 5 minutes
def load_table(query: str) -> pd.DataFrame:
    con = get_connection()
    return con.execute(query).df()


def table_exists(name: str) -> bool:
    con = get_connection()
    result = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [name],
    ).fetchone()
    return result[0] > 0


# ── Sidebar ───────────────────────────────────────────────────────────────

st.sidebar.title("🎮 Esports Pipeline")
st.sidebar.markdown("Live data from a Prefect-orchestrated ETL pipeline ingesting "
                     "League of Legends and CS2 match data on a 6-hour schedule.")

page = st.sidebar.radio(
    "View",
    ["Overview", "League of Legends", "Counter-Strike 2", "Pipeline Health"],
)

st.sidebar.markdown("---")
st.sidebar.markdown(
    "[GitHub Repo](https://github.com/clb158/esports-data-pipeline)  \n"
    "Built with Python · Prefect · DuckDB · Streamlit"
)

# ── Shared header ─────────────────────────────────────────────────────────

st.title("Esports Analytics Pipeline")
st.caption("Bronze → Silver → Gold medallion architecture | Updated automatically every 6 hours")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# OVERVIEW
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if page == "Overview":

    col1, col2, col3, col4 = st.columns(4)

    lol_matches = load_table("SELECT COUNT(DISTINCT match_id) AS n FROM lol_match_stats")["n"][0] \
        if table_exists("lol_match_stats") else 0
    cs_matches = load_table("SELECT COUNT(DISTINCT match_id) AS n FROM cs_match_stats")["n"][0] \
        if table_exists("cs_match_stats") else 0
    lol_players = load_table("SELECT COUNT(*) AS n FROM lol_player_agg")["n"][0] \
        if table_exists("lol_player_agg") else 0
    cs_teams = load_table("SELECT COUNT(*) AS n FROM cs_team_agg")["n"][0] \
        if table_exists("cs_team_agg") else 0

    col1.metric("LoL Matches Tracked", f"{lol_matches:,}")
    col2.metric("CS2 Matches Tracked", f"{cs_matches:,}")
    col3.metric("LoL Players", f"{lol_players:,}")
    col4.metric("CS2 Teams", f"{cs_teams:,}")

    st.markdown("---")

    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Top LoL Players by KDA")
        if table_exists("lol_player_agg"):
            df = load_table("""
                SELECT summoner_name, avg_kda, win_rate, games_played, top_champion
                FROM lol_player_agg
                ORDER BY avg_kda DESC
                LIMIT 10
            """)
            if not df.empty:
                fig = px.bar(
                    df, x="avg_kda", y="summoner_name",
                    orientation="h", color="win_rate",
                    color_continuous_scale="Blues",
                    labels={"avg_kda": "Average KDA", "summoner_name": "", "win_rate": "Win Rate"},
                )
                fig.update_layout(yaxis={"categoryorder": "total ascending"}, height=400)
                st.plotly_chart(fig, width='stretch')
            else:
                st.info("No player data yet — waiting on the next pipeline run.")
        else:
            st.info("Warehouse not initialised yet.")

    with c2:
        st.subheader("CS2 Team Win Rates")
        if table_exists("cs_team_agg"):
            df = load_table("""
                SELECT team_name, win_rate, matches_played, avg_rounds_won
                FROM cs_team_agg
                ORDER BY win_rate DESC
                LIMIT 10
            """)
            if not df.empty:
                fig = px.bar(
                    df, x="win_rate", y="team_name",
                    orientation="h", color="matches_played",
                    color_continuous_scale="Oranges",
                    labels={"win_rate": "Win Rate", "team_name": "", "matches_played": "Matches"},
                )
                fig.update_layout(yaxis={"categoryorder": "total ascending"}, height=400)
                fig.update_xaxes(tickformat=".0%")
                st.plotly_chart(fig, width='stretch')
            else:
                st.info("No team data yet — waiting on the next pipeline run.")
        else:
            st.info("Warehouse not initialised yet.")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LEAGUE OF LEGENDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

elif page == "League of Legends":

    st.header("League of Legends")

    if not table_exists("lol_player_agg"):
        st.warning("No LoL data in the warehouse yet.")
    else:
        df = load_table("""
            SELECT summoner_name, games_played, win_rate, avg_kda, avg_kills,
                   avg_deaths, avg_assists, avg_cs_per_min, avg_vision, top_champion
            FROM lol_player_agg
            ORDER BY avg_kda DESC
        """)

        st.subheader("Player Leaderboard")
        st.dataframe(
            df.style.format({
                "win_rate": "{:.1%}",
                "avg_kda": "{:.2f}",
                "avg_kills": "{:.1f}",
                "avg_deaths": "{:.1f}",
                "avg_assists": "{:.1f}",
                "avg_cs_per_min": "{:.1f}",
                "avg_vision": "{:.1f}",
            }),
            width='stretch',
            hide_index=True,
        )

        st.markdown("---")
        c1, c2 = st.columns(2)

        with c1:
            st.subheader("KDA vs Win Rate")
            if not df.empty:
                fig = px.scatter(
                    df, x="avg_kda", y="win_rate", size="games_played",
                    hover_name="summoner_name", color="avg_cs_per_min",
                    labels={"avg_kda": "Average KDA", "win_rate": "Win Rate"},
                    color_continuous_scale="Viridis",
                )
                fig.update_yaxes(tickformat=".0%")
                st.plotly_chart(fig, width='stretch')

        with c2:
            st.subheader("Most Played Champions")
            champ_df = load_table("""
                SELECT champion_name, COUNT(*) AS picks
                FROM lol_match_stats
                GROUP BY champion_name
                ORDER BY picks DESC
                LIMIT 10
            """)
            if not champ_df.empty:
                fig = px.pie(champ_df, names="champion_name", values="picks", hole=0.4)
                st.plotly_chart(fig, width='stretch')

        st.markdown("---")
        st.subheader("Recent Matches")
        recent = load_table("""
            SELECT match_id, summoner_name, champion_name, win, kills, deaths,
                   assists, kda, cs_per_min, match_ts
            FROM lol_match_stats
            ORDER BY processed_at DESC
            LIMIT 25
        """)
        st.dataframe(recent, width='stretch', hide_index=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COUNTER-STRIKE 2
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

elif page == "Counter-Strike 2":

    st.header("Counter-Strike 2")

    if not table_exists("cs_team_agg"):
        st.warning("No CS2 data in the warehouse yet.")
    else:
        df = load_table("""
            SELECT team_name, matches_played, win_rate, avg_rounds_won, avg_rounds_lost
            FROM cs_team_agg
            ORDER BY win_rate DESC
        """)

        st.subheader("Team Standings")
        st.dataframe(
            df.style.format({
                "win_rate": "{:.1%}",
                "avg_rounds_won": "{:.1f}",
                "avg_rounds_lost": "{:.1f}",
            }),
            width='stretch',
            hide_index=True,
        )

        st.markdown("---")
        c1, c2 = st.columns(2)

        with c1:
            st.subheader("Rounds Won vs Lost (avg)")
            if not df.empty:
                fig = go.Figure()
                fig.add_trace(go.Bar(name="Avg Rounds Won", x=df["team_name"], y=df["avg_rounds_won"]))
                fig.add_trace(go.Bar(name="Avg Rounds Lost", x=df["team_name"], y=df["avg_rounds_lost"]))
                fig.update_layout(barmode="group", height=420)
                st.plotly_chart(fig, width='stretch')

        with c2:
            st.subheader("Matches by Tournament")
            tourney_df = load_table("""
                SELECT tournament_name, COUNT(DISTINCT match_id) AS matches
                FROM cs_match_stats
                GROUP BY tournament_name
                ORDER BY matches DESC
            """)
            if not tourney_df.empty:
                fig = px.bar(tourney_df, x="tournament_name", y="matches",
                             labels={"tournament_name": "", "matches": "Matches"})
                st.plotly_chart(fig, width='stretch')

        st.markdown("---")
        st.subheader("Recent Matches")
        recent = load_table("""
            SELECT match_id, team_name, opponent_name, win, score,
                   tournament_name, match_ts
            FROM cs_match_stats
            ORDER BY processed_at DESC
            LIMIT 25
        """)
        st.dataframe(recent, width='stretch', hide_index=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PIPELINE HEALTH
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

elif page == "Pipeline Health":

    st.header("Pipeline Health")
    st.caption("Observability into the Prefect-orchestrated ETL runs — proves the pipeline "
               "is actually running on schedule, not just that it ran once.")

    if not table_exists("pipeline_runs"):
        st.warning("No pipeline run history yet.")
    else:
        runs = load_table("""
            SELECT flow_name, status, rows_ingested, rows_transformed,
                   started_at, finished_at, error_msg
            FROM pipeline_runs
            ORDER BY started_at DESC
            LIMIT 50
        """)

        col1, col2, col3 = st.columns(3)
        total_runs = len(runs)
        success_runs = len(runs[runs["status"] == "success"]) if not runs.empty else 0
        failed_runs = len(runs[runs["status"] == "failed"]) if not runs.empty else 0

        col1.metric("Total Runs Logged", total_runs)
        col2.metric("Successful", success_runs)
        col3.metric("Failed", failed_runs, delta=None,
                     delta_color="inverse" if failed_runs > 0 else "off")

        st.markdown("---")
        st.subheader("Run History")

        def status_color(val):
            if val == "success":
                return "background-color: #d4edda"
            elif val == "failed":
                return "background-color: #f8d7da"
            elif val == "dq_warning":
                return "background-color: #fff3cd"
            return ""

        if not runs.empty:
            st.dataframe(
                runs.style.map(status_color, subset=["status"]),
                width='stretch',
                hide_index=True,
            )
        else:
            st.info("No runs logged yet.")

        st.markdown("---")
        st.subheader("Data Quality Checks")

        if table_exists("data_quality_checks"):
            dq = load_table("""
                SELECT table_name, check_name, passed, metric_value, threshold, checked_at
                FROM data_quality_checks
                ORDER BY checked_at DESC
                LIMIT 50
            """)
            if not dq.empty:
                fail_count = len(dq[dq["passed"] == False])
                if fail_count > 0:
                    st.error(f"{fail_count} failed check(s) in the last 50 — review below.")
                else:
                    st.success("All recent data quality checks passed.")

                st.dataframe(
                    dq.style.map(
                        lambda v: "background-color: #f8d7da" if v == False else "",
                        subset=["passed"],
                    ),
                    width='stretch',
                    hide_index=True,
                )
            else:
                st.info("No DQ checks logged yet.")

st.markdown("---")
st.caption(f"Dashboard loaded at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} "
           "· Data refreshes from cache every 5 minutes")
