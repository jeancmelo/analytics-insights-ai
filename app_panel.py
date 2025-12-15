import os
import re
import time
from datetime import date, timedelta

import pandas as pd
import streamlit as st

# =========================================================
# EMBED / IFRAME SUPPORT (MVP)
# =========================================================
try:
    from streamlit.web.server import websocket_headers as wh

    _orig_get = wh._get_websocket_headers

    def _patched_get(*args, **kwargs):
        headers = _orig_get(*args, **kwargs)
        # MVP: allow embedding anywhere (lock down later)
        headers["Content-Security-Policy"] = "frame-ancestors *"
        headers.pop("X-Frame-Options", None)
        return headers

    wh._get_websocket_headers = _patched_get
except Exception:
    pass

# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(
    page_title="AI Insights Panel",
    layout="wide",
)

# =========================================================
# ENV VARS
# =========================================================
OPENAI_KEY = os.getenv("OPENAI_API", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

SHOW_SQL = os.getenv("SHOW_SQL", "0") == "1"
SHOW_TABLE = os.getenv("SHOW_TABLE", "0") == "1"

# =========================================================
# OPENAI
# =========================================================
from openai import OpenAI
import httpx

client = None
if OPENAI_KEY:
    http_client = httpx.Client(
        timeout=60.0,
        follow_redirects=True,
        trust_env=False,
    )
    client = OpenAI(api_key=OPENAI_KEY, http_client=http_client)

# =========================================================
# UI — HEADER
# =========================================================
st.markdown(
    """
    <style>
    html, body, .stApp { background:#ffffff; color:#0f172a; }
    .block-container { max-width: 900px; padding-top: 1rem; }

    .panel-title {
        font-size: 20px;
        font-weight: 700;
        margin-bottom: .5rem;
    }

    .panel-sub {
        color:#64748b;
        margin-bottom: 1rem;
    }

    .qa {
        background:#ffffff;
        border:1px solid #e5e7eb;
        border-radius:14px;
        padding:14px 16px;
        margin-bottom:12px;
        box-shadow:0 6px 18px rgba(31,41,55,.08);
    }

    .qa-q {
        background:#eef2ff;
        padding:10px 12px;
        border-radius:10px;
        font-weight:600;
        margin-bottom:10px;
    }

    .qa-a {
        font-size:0.95rem;
        line-height:1.45rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown('<div class="panel-title">AI Insights</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="panel-sub">Ask questions about performance and trends</div>',
    unsafe_allow_html=True,
)

# =========================================================
# DATA SOURCE SELECT
# =========================================================
SOURCE_MAP = {
    "gsc": "Google Search Console (BigQuery)",
    "ig": "Instagram Insights (Supermetrics)",
    "fb": "Facebook Page Insights (Supermetrics)",
}

source_label = st.selectbox(
    "Data source",
    list(SOURCE_MAP.values()),
)

# =========================================================
# DATE RANGE (SIMPLE, LOCAL)
# =========================================================
with st.expander("Date range", expanded=False):
    col1, col2 = st.columns(2)
    with col1:
        date_from = st.date_input(
            "From",
            value=date.today() - timedelta(days=30),
        )
    with col2:
        date_to = st.date_input(
            "To",
            value=date.today(),
        )

# =========================================================
# INPUT
# =========================================================
question = st.text_area(
    "Ask a question",
    placeholder="Ex: What changed in the last 30 days?",
    height=70,
)

send = st.button("Generate insights", use_container_width=True)

# =========================================================
# HELPERS
# =========================================================
def ai_summary(question: str, df: pd.DataFrame, context: str) -> str:
    if not client:
        return "OPENAI_API is not configured."

    if df.empty:
        return "No data available for this period."

    preview = df.head(25).to_csv(index=False)

    system = (
        "You are a senior digital analytics consultant. "
        "Write a concise, professional insight based only on the data."
    )

    user = (
        f"Question:\n{question}\n\n"
        f"Context:\n{context}\n\n"
        f"Data sample (CSV):\n{preview}"
    )

    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.2,
    )

    return resp.choices[0].message.content.strip()


# =========================================================
# MAIN LOGIC
# =========================================================
if send and question.strip():
    with st.spinner("Generating insights…"):

        # -------------------------------
        # GOOGLE SEARCH CONSOLE (BQ)
        # -------------------------------
        if source_label.startswith("Google"):
            from google.cloud import bigquery

            BQ_TABLE = os.getenv("BQ_TABLE", "").strip()
            SA_JSON = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON", "").strip()

            if not BQ_TABLE or not SA_JSON:
                st.error("BigQuery credentials not configured.")
                st.stop()

            sa_path = "/tmp/sa.json"
            with open(sa_path, "w") as f:
                f.write(SA_JSON)

            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path
            bq = bigquery.Client()

            sql = f"""
            SELECT
              date AS date,
              SUM(clicks) AS clicks,
              SUM(impressions) AS impressions
            FROM `{BQ_TABLE}`
            WHERE date BETWEEN '{date_from}' AND '{date_to}'
            GROUP BY date
            ORDER BY date
            """

            df = bq.query(sql).to_dataframe()
            context = f"GSC data from {date_from} to {date_to}"

        # -------------------------------
        # INSTAGRAM (SUPERMETRICS)
        # -------------------------------
        elif source_label.startswith("Instagram"):
            from supermetrics_adapter import instagram_adapter_from_env

            ig = instagram_adapter_from_env()

            df = ig.query(
                fields=["date", "followers_count", "follows_count"],
                date_from=str(date_from),
                date_to=str(date_to),
            )

            context = f"Instagram Insights from {date_from} to {date_to}"

        # -------------------------------
        # FACEBOOK (SUPERMETRICS)
        # -------------------------------
        else:
            from supermetrics_adapter import facebook_adapter_from_env

            fb = facebook_adapter_from_env()

            df = fb.query(
                fields=["date", "page_engaged_users"],
                date_from=str(date_from),
                date_to=str(date_to),
            )

            context = f"Facebook Page Insights from {date_from} to {date_to}"

        # -------------------------------
        # AI RESPONSE
        # -------------------------------
        answer = ai_summary(question, df, context)

        st.markdown('<div class="qa">', unsafe_allow_html=True)
        st.markdown(f'<div class="qa-q">{question}</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="qa-a">{answer}</div>', unsafe_allow_html=True)

        if SHOW_SQL and source_label.startswith("Google"):
            st.code(sql, language="sql")

        if SHOW_TABLE:
            st.dataframe(df, use_container_width=True)
