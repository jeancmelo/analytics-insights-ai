import os, re, json, time
from datetime import date, timedelta
import pandas as pd
import streamlit as st
from html import escape
from supermetrics_adapter import (
    instagram_adapter_from_env,
    facebook_pages_adapter_from_env,
)

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
BQ_TABLE     = os.getenv("BQ_TABLE", "").strip()
SA_JSON      = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON", "").strip()
OPENAI_KEY   = os.getenv("OPENAI_API", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

if not BQ_TABLE:  st.error("Defina BQ_TABLE (ex.: projeto.dataset.tabela).")
if not SA_JSON:   st.error("Defina GOOGLE_APPLICATION_CREDENTIALS_JSON.")
if not OPENAI_KEY: st.warning("Defina OPENAI_API para habilitar a IA.")

# --------- Credencial GCP ---------
if SA_JSON:
    SA_PATH = "/tmp/sa.json"
    with open(SA_PATH, "w") as f:
        f.write(SA_JSON)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_PATH

# --------- BigQuery ---------
from google.cloud import bigquery
@st.cache_resource(show_spinner=False)
def get_bq(): return bigquery.Client()
bq = get_bq() if SA_JSON else None

@st.cache_data(show_spinner=False)
def get_table_schema(table_fqn: str):
    tbl = bq.get_table(table_fqn)
    return [(s.name, s.field_type) for s in tbl.schema]

# --------- OpenAI ---------
from openai import OpenAI
import httpx
client = None
if OPENAI_KEY:
    http_client = httpx.Client(timeout=60.0, follow_redirects=True, trust_env=False)
    client = OpenAI(api_key=OPENAI_KEY, http_client=http_client)

# --------- STYLE (normalizado p/ caixas do Looker Studio) ---------
st.markdown("""
<style>
body, .stApp {
    background: #ffffff !important;
    color: #111111 !important;
    font-family: "Segoe UI", Arial, sans-serif;
}

/* Caixa padrão */
.box {
    background: #ffffff !important;
    border: 1px solid #d1d5db !important;
    border-radius: 8px;
    padding: 16px;
    margin-bottom: 16px;
    color: #111111 !important;
}

/* Divisor */
.divider {
    height: 1px;
    background: #e5e7eb;
    margin: 1rem 0;
}

/* Botões rápidos (chips) */
.chip-group {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 8px;
}
.chip-btn button {
    background: #ffffff !important;
    color: #111111 !important;
    border: 1px solid #d1d5db !important;
    border-radius: 6px;
    padding: 6px 10px;
}
.chip-btn button:hover {
    background: #f9fafb !important;
    border-color: #9ca3af !important;
}

/* Botões principais */
.btn-primary button {
    background: #2563eb !important;
    color: #fff !important;
    border-radius: 6px;
    padding: 8px 12px;
    border: none;
}
.btn-primary button:hover { background: #1e40af !important; }

.btn-secondary button {
    background: #ffffff !important;
    color: #111111 !important;
    border-radius: 6px;
    border: 1px solid #d1d5db;
    padding: 8px 12px;
}
.btn-secondary button:hover { background: #f9fafb !important; }

/* Corrige todos os botões (Send, Clear, Quick Prompts) */
button[data-testid="baseButton-secondary"],
button[data-testid="baseButton-primary"] {
    background-color: #ffffff !important;   /* fundo branco */
    color: #111111 !important;              /* texto preto */
    border: 1px solid #d1d5db !important;   /* borda cinza */
    border-radius: 6px !important;
    padding: 6px 12px !important;
    box-shadow: none !important;
}

/* Corrige textarea do Streamlit */
div[data-testid="stTextArea"] textarea,
div[data-testid="stTextInput-RootElement"] textarea {
    background-color: #ffffff !important;  /* fundo branco */
    color: #111111 !important;             /* texto preto */
    border: 1px solid #d1d5db !important;  /* borda cinza clara */
    border-radius: 6px !important;
    padding: 8px !important;
    font-size: 14px !important;
}

/* Placeholder em cinza */
div[data-testid="stTextArea"] textarea::placeholder,
div[data-testid="stTextInput-RootElement"] textarea::placeholder {
    color: #6b7280 !important;  /* cinza médio */
}


/* Hover dos botões */
button[data-testid="baseButton-secondary"]:hover,
button[data-testid="baseButton-primary"]:hover {
    background-color: #f9fafb !important;   /* cinza bem claro */
    border-color: #9ca3af !important;
    color: #111111 !important;
}


/* Insights */
.insights-card {
    background: #ffffff !important;
    border: 1px solid #d1d5db !important;
    border-radius: 8px;
    padding: 16px;
    margin-top: 16px;
    color: #111111 !important;
}
.kf-title {
    font-weight: 700;
    margin-bottom: 10px;
}
.kf-list {
    counter-reset: item;
    list-style: none;
    padding-left: 0;
}
.kf-list li {
    counter-increment: item;
    margin: 0.5rem 0;
    color: #111111 !important;
}
.kf-list li::before {
    content: counter(item) ".";
    font-weight: 700;
    margin-right: 6px;
    color: #111111 !important;
}
.kf-item-title { font-weight: 600; }
.kf-item-text { color: #111111 !important; }
</style>
""", unsafe_allow_html=True)

# --------- Helpers ---------
def sanitize_sql(text: str) -> str:
    if not text: return ""
    t = text.strip()
    t = re.sub(r"^sql\s*", "", t, flags=re.IGNORECASE)
    t = re.sub(r"^```(?:sql)?\s*|\s*```$", "", t, flags=re.IGNORECASE|re.DOTALL)
    m = re.search(r"\bselect\b", t, flags=re.IGNORECASE)
    if m: t = t[m.start():]
    return t.strip().rstrip(";")

def sql_is_safe(sql: str) -> bool:
    s = sql.strip().lower()
    if not re.match(r"^\s*select\b", s): return False
    forbidden = ["insert","update","delete","merge","drop","create","alter","truncate",";","--","/*"]
    if any(tok in s for tok in forbidden): return False
    target_clean = re.sub(r"[`\s]","", BQ_TABLE.lower())
    s_clean      = re.sub(r"[`\s]","", s)
    return target_clean in s_clean

def ensure_limit(sql: str, default_limit:int=1000) -> str:
    return sql if re.search(r"\blimit\b\s+\d+\s*$", sql, re.I) else f"{sql}\nLIMIT {default_limit}"

# --------- AI SQL + Findings ---------
def build_sql_with_ai(question: str, table_fqn: str, columns: list) -> str:
    if not client: return ""
    cols_txt = "\n".join([f"- {c} ({t})" for c, t in columns])
    system = "Você é um gerador de SQL para BigQuery. Responda SOMENTE com a consulta SQL."
    user = f"Tabela: `{table_fqn}`.\nColunas:\n{cols_txt}\n\nPergunta:\n{question}\n"
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{"role":"system","content":system},{"role":"user","content":user}],
        temperature=0.1,
    )
    return sanitize_sql(resp.choices[0].message.content.strip())

def ai_key_findings(question: str, df: pd.DataFrame, sql_used: str, n:int=5):
    if not client: return [{"title":"Configuração necessária","text":"Defina OPENAI_API."}]
    if df.empty:   return [{"title":"Sem dados","text":"Não há linhas para o recorte solicitado."}]
    preview = df.head(40).to_csv(index=False)
    system = "Você é um analista de Marketing/SEO. Gere insights curtos em JSON válido."
    user = f"Gere até {n} findings. Pergunta:\n{question}\nSQL:\n{sql_used}\nPrévia:\n{preview}"
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{"role":"system","content":system},{"role":"user","content":user}],
        temperature=0.2,
        response_format={"type":"json_object"}
    )
    try:
        data = json.loads(resp.choices[0].message.content or "{}")
        return [{"title": it.get("title","Insight"), "text": it.get("text","")} for it in data.get("findings", [])[:n]]
    except Exception:
        return [{"title":"Resumo", "text": resp.choices[0].message.content.strip()}]

# --------- STATE ---------
if "insights" not in st.session_state: st.session_state.insights = []
if "pending" not in st.session_state: st.session_state.pending = None

# --------- UI ---------
st.markdown("### Generative Insights")
with st.container():
    st.markdown('<div class="box">', unsafe_allow_html=True)

    source = st.selectbox(
        "Data source",
        ["Google Search Console (BigQuery)",
         "Instagram Insights (Supermetrics)",
         "Facebook Page Insights (Supermetrics)"],
        index=0
    )

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    st.caption("Quick prompts")
    col1, col2 = st.columns(2)
    with col1: chip1 = st.button("Key findings for this period", key="chip1")
    with col2: chip2 = st.button("Compare with last month", key="chip2")
    col3, col4 = st.columns(2)
    with col3: chip3 = st.button("Top queries & pages", key="chip3")
    with col4: chip4 = st.button("Any anomalies to highlight?", key="chip4")

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    st.caption("Type your question")
    q = st.text_area(" ", key="ask", height=90,
                     placeholder="e.g., Give me 5 actionable insights for this dataset.")

    st.markdown('<div class="btn-primary">', unsafe_allow_html=True)
    send = st.button("Send", use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="btn-secondary">', unsafe_allow_html=True)
    clear = st.button("Clear insights", use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)

# --------- Lógica chips ---------
if chip1: q, send = "Give me 5 key findings for the current period.", True
if chip2: q, send = "Summarize performance vs last month.", True
if chip3: q, send = "Top queries and pages this period.", True
if chip4: q, send = "Detect anomalies or significant changes.", True

# --------- Limpar ---------
if clear:
    st.session_state.insights, st.session_state.pending = [], None
    st.rerun()

# --------- Enfileira ---------
if send and q and q.strip():
    st.session_state.insights.insert(0, {"q": q.strip(), "findings": None, "ts": time.time(), "sql": None})
    st.session_state.pending = 0
    st.rerun()

# --------- Processa ---------
if st.session_state.pending is not None:
    idx = st.session_state.pending
    try:
        q_user = st.session_state.insights[idx]["q"]
        current_source = source

        if current_source.startswith("Google Search Console"):
            schema_cols = get_table_schema(BQ_TABLE) if bq else []
            sql = build_sql_with_ai(q_user, BQ_TABLE, schema_cols)
            if not sql or not sql_is_safe(sql):
                findings = [{"title":"Consulta inválida","text":"Não foi possível gerar SQL segura."}]
            else:
                sql = ensure_limit(sql)
                df  = bq.query(sql).result().to_dataframe()
                findings = ai_key_findings(q_user, df, sql, n=6)
            st.session_state.insights[idx].update({"findings": findings, "sql": sql})

        elif current_source.startswith("Instagram"):
            ig = instagram_adapter_from_env()
            fields_env = os.getenv("IGI_FIELDS","month,followers_count").split(",")
            end = date.today(); start = end - timedelta(days=30)
            df = ig.query(fields=fields_env, date_from=start.isoformat(), date_to=end.isoformat())
            findings = ai_key_findings(q_user, df, "Supermetrics IGI", n=6)
            st.session_state.insights[idx].update({"findings": findings, "sql": "Supermetrics (IGI)"})

        elif current_source.startswith("Facebook"):
            fb = facebook_pages_adapter_from_env()
            end = date.today(); start = end - timedelta(days=30)
            fields = os.getenv("FPI_FIELDS","date,page_id,post_id,post_reach").split(",")
            df = fb.query(fields=fields, date_from=start.isoformat(), date_to=end.isoformat())
            findings = ai_key_findings(q_user, df, "Supermetrics FB", n=6)
            st.session_state.insights[idx].update({"findings": findings, "sql": "Supermetrics (FB)"})

    except Exception as e:
        st.session_state.insights[idx].update({"findings":[{"title":"Erro","text":str(e)}], "sql":""})
    finally:
        st.session_state.pending = None
        st.rerun()

# --------- Render ---------
if st.session_state.insights:
    block = st.session_state.insights[0]
    st.markdown('<div class="insights-card">', unsafe_allow_html=True)
    st.markdown('<div class="kf-title">Key Findings</div>', unsafe_allow_html=True)
    if block["findings"] is None:
        st.write("Gerando insights…")
    else:
        st.markdown('<ol class="kf-list">', unsafe_allow_html=True)
        for it in block["findings"]:
            st.markdown(
                f'<li><span class="kf-item-title">{escape(it.get("title",""))}</span> '
                f'<span class="kf-item-text">{escape(it.get("text",""))}</span></li>',
                unsafe_allow_html=True
            )
        st.markdown('</ol>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    with st.expander("SQL usada (debug)"):
        st.code(block.get("sql") or "", language="sql")
else:
    st.info("Use os quick prompts acima ou escreva sua pergunta e clique em **Send**.")
