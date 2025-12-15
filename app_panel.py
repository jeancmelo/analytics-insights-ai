import os, re, time
from datetime import date, timedelta
import pandas as pd
import streamlit as st

# ===============================
# --- NonEmbed / Looker-hosted mode ---
# Expected params:
#   ?source=gsc|ig|fb&date_from=YYYY-MM-DD&date_to=YYYY-MM-DD
params = st.query_params

def _qp(key: str) -> str:
    v = params.get(key)
    if isinstance(v, list):
        return (v[0] or "").strip()
    return (v or "").strip()

QP_SOURCE = _qp("source").lower()      # gsc | ig | fb
QP_DATE_FROM = _qp("date_from")        # YYYY-MM-DD
QP_DATE_TO = _qp("date_to")            # YYYY-MM-DD

def _is_iso_date(s: str) -> bool:
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", (s or "").strip()))

USE_QP_DATES = _is_iso_date(QP_DATE_FROM) and _is_iso_date(QP_DATE_TO)
# ===============================


# ========== EMBED NO LOOKER STUDIO ==========
try:
    from streamlit.web.server import websocket_headers as wh
    _orig_get = wh._get_websocket_headers
    def _patched_get(*args, **kwargs):
        headers = _orig_get(*args, **kwargs)
        headers["Content-Security-Policy"] = (
            "frame-ancestors 'self' https://lookerstudio.google.com https://datastudio.google.com"
        )
        headers.pop("X-Frame-Options", None)
        return headers
    wh._get_websocket_headers = _patched_get
except Exception:
    pass
# ============================================

st.set_page_config(page_title="AI Insights Panel", layout="wide")


# --------- ENV VARS ---------
OPENAI_KEY   = os.getenv("OPENAI_API", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

# --------- OpenAI ---------
from openai import OpenAI
import httpx
client = None
if OPENAI_KEY:
    http_client = httpx.Client(timeout=60.0, follow_redirects=True, trust_env=False)
    client = OpenAI(api_key=OPENAI_KEY, http_client=http_client)


# ===============================
# UI — Data Source (com default via query param)
# ===============================
source_map = {
    "gsc": "Google Search Console (BigQuery)",
    "ig": "Instagram Insights (Supermetrics)",
    "fb": "Facebook Page Insights (Supermetrics)",
}

default_source_key = QP_SOURCE if QP_SOURCE in source_map else "gsc"
source_labels = list(source_map.values())
default_index = list(source_map.keys()).index(default_source_key)

current_source = st.selectbox(
    "Data source",
    source_labels,
    index=default_index
)

# ===============================
# Contexto de datas (NonEmbed)
# ===============================
if USE_QP_DATES:
    st.caption(f"Contexto do dashboard: {QP_DATE_FROM} → {QP_DATE_TO}")

# ===============================
# Lógica principal (mantida)
# ===============================

# aqui tu continua com:
# - ai_key_findings
# - adapters (GSC / Instagram / Facebook)
# - UI dos insights
# Só muda o ponto onde defines datas 👇


# ===============================
# Exemplo: Instagram Adapter
# ===============================
if current_source.startswith("Instagram"):
    from supermetrics_adapter import instagram_adapter_from_env
    ig = instagram_adapter_from_env()

    fields = os.getenv("IGI_FIELDS", "month,followers_count,follows_count").split(",")

    if USE_QP_DATES:
        df = ig.query(
            fields=[f.strip() for f in fields],
            date_from=QP_DATE_FROM,
            date_to=QP_DATE_TO
        )
        sql_ctx = f"Supermetrics IGI {QP_DATE_FROM}..{QP_DATE_TO}"
    else:
        end = date.today()
        start = end - timedelta(days=30)
        df = ig.query(
            fields=[f.strip() for f in fields],
            date_from=start.isoformat(),
            date_to=end.isoformat()
        )
        sql_ctx = f"Supermetrics IGI {start}..{end}"

    # segue teu fluxo normal de AI insights
