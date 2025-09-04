import os, re, json, time, html
from datetime import date, timedelta
import pandas as pd
import streamlit as st

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

# --------- ENV ---------
BQ_TABLE     = os.getenv("BQ_TABLE", "").strip()
SA_JSON      = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON", "").strip()
OPENAI_KEY   = os.getenv("OPENAI_API", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()
PANEL_WIDTH  = int(os.getenv("PANEL_WIDTH", "380"))  # largura da coluna (px)

if not BQ_TABLE:  st.error("Defina BQ_TABLE (ex.: projeto.dataset.tabela).")
if not SA_JSON:   st.error("Defina GOOGLE_APPLICATION_CREDENTIALS_JSON (conteúdo do JSON da Service Account).")
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

# --------- STYLE (tema claro, coluna lateral “tipo print”) ---------
st.markdown("""
<style>
/* Base clara e coluna esquerda */
html, body, .stApp, [data-testid="stAppViewContainer"], .main {{
  background:#ffffff !important; color:#0f172a;
}}
/* a block-container ocupa 100% da largura disponível do iframe,
   mas fica limitada a --panel-w e alinhada à esquerda (sem centralizar) */
:root {{ --panel-w: {PANEL_WIDTH}px; }}
.main .block-container {{
  max-width: var(--panel-w);
  width: 100%;
  padding-top: .6rem;
  margin-left: 0 !important;
  margin-right: auto !important;
}}

/* cards */
.card {{
  background:#ffffff; border:1px solid #e5e7eb; border-radius:14px;
  box-shadow:0 6px 18px rgba(31,41,55,.08);
}}
.panel-card {{ padding:12px 14px; margin-bottom:12px; }}
.kf-card    {{ padding:12px 14px; }}

/* sticky (opcional) – mantém os controles visíveis quando rola */
.sticky {{ position: sticky; top: 8px; z-index: 2; }}

/* títulos */
h3 {{ margin:.1rem 0 .6rem 0; font-weight:700; }}

/* selects e inputs */
[data-baseweb="select"]>div{{ border-radius:10px; }}
textarea, .stTextArea textarea {{
  min-height:74px !important; background:#ffffff !important; color:#111827 !important;
  border:1px solid #e5e7eb !important; border-radius:10px !important;
}}
textarea::placeholder{{ color:#334155 !important; opacity:1 !important; }}
textarea:focus{{ outline:none !important; border-color:#94a3b8 !important;
  box-shadow:0 0 0 3px rgba(37,99,235,.15) !important; }}
/* Key Findings – lista numerada elegante */
.kf-title{{ font-weight:700; margin-bottom:.4rem; }}
.kf-list{{ counter-reset:item; list-style:none; padding-left:0; margin:0; }}
.kf-list li{{ counter-increment:item; margin:.55rem 0; }}
.kf-list li::before{{
  content: counter(item) ".";
  font-weight:700; margin-right:.35rem; color:#111827;
}}
.kf-item-title{{ font-weight:700; display:inline; }}
.kf-item-text{{ display:block; margin-top:.15rem; color:#0f172a; }}

/* divisória */
.divider{{ height:1px; background:#e5e7eb; margin:.6rem 0; }}

/* ==== BOTÕES CLAROS (força geral) ==== */
:root{
  --btn-bg:#f8fafc;          /* branco suave */
  --btn-bg-hover:#f1f5f9;    /* hover */
  --btn-text:#111827;        /* texto preto */
  --btn-border:#e5e7eb;      /* borda cinza clara */
}

/* pega todos os tipos de botão do Streamlit */
[data-testid="stAppViewContainer"] button,
[data-testid="stAppViewContainer"] .stButton > button,
[data-testid="baseButton-primary"],
[data-testid="baseButton-secondary"],
[data-testid="stBaseButton-primary"],
[data-testid="stBaseButton-secondary"],
.chips .stButton > button,
.btn-primary .stButton > button,
.btn-secondary .stButton > button {
  background: var(--btn-bg) !important;
  background-color: var(--btn-bg) !important;
  color: var(--btn-text) !important;
  border: 1px solid var(--btn-border) !important;
  box-shadow: none !important;
  border-radius: 10px !important;
}

/* alguns temas colocam o texto do botão dentro de <p>/<span> */
.stButton > button p,
.stButton > button span {
  color: var(--btn-text) !important;
}

[data-testid="stAppViewContainer"] button:hover,
[data-testid="stAppViewContainer"] .stButton > button:hover,
.chips .stButton > button:hover,
.btn-primary .stButton > button:hover,
.btn-secondary .stButton > button:hover {
  background: var(--btn-bg-hover) !important;
  background-color: var(--btn-bg-hover) !important;
  border-color: #cbd5e1 !important;
}

/* estado desabilitado (ex.: enquanto processa) */
[data-testid="stAppViewContainer"] button:disabled,
[data-testid="stAppViewContainer"] .stButton > button:disabled {
  background:#f3f4f6 !important; color:#9ca3af !important; border-color:#e5e7eb !important; opacity:1 !important;
}
.st-emotion-cache-13k62yr { background-color: #ffffff; }
.st-bx {background-color: #f8fafc }
.st-emotion-cache-103r2r1 { gap: 3px !important; }
p { color: #000 !important; }
.st-emotion-cache-1sy6v2f  { background-color: #ffffff;  border: 1px solid #cbd5e1; }
</style>
""", unsafe_allow_html=True)


# --------- Helpers: SQL/LLM ---------
def sanitize_sql(text: str) -> str:
    if not text: return ""
    t = text.strip()
    t = re.sub(r"^sql\\s*", "", t, flags=re.IGNORECASE)
    t = re.sub(r"^```(?:sql)?\\s*|\\s*```$", "", t, flags=re.IGNORECASE|re.DOTALL)
    m = re.search(r"\\bselect\\b", t, flags=re.IGNORECASE)
    if m: t = t[m.start():]
    return t.strip().rstrip(";")

def sql_is_safe(sql: str) -> bool:
    s = sql.strip().lower()
    if not re.match(r"^\\s*select\\b", s): return False
    forbidden = ["insert","update","delete","merge","drop","create","alter","truncate",";","--","/*"]
    if any(tok in s for tok in forbidden): return False
    target_clean = re.sub(r"[`\\s]","", BQ_TABLE.lower())
    s_clean      = re.sub(r"[`\\s]","", s)
    return target_clean in s_clean

def ensure_limit(sql: str, default_limit:int=1000) -> str:
    return sql if re.search(r"\\blimit\\b\\s+\\d+\\s*$", sql, re.I) else f"{sql}\\nLIMIT {default_limit}"

from openai import OpenAI
def build_sql_with_ai(question: str, table_fqn: str, columns: list) -> str:
    if not client: return ""
    cols_txt = "\\n".join([f"- {c} ({t})" for c, t in columns])
    system = (
        "Você é um gerador de SQL para BigQuery. "
        "Responda SOMENTE com a consulta SQL (sem rótulos, sem explicações, sem cercas de código). "
        "Use exclusivamente a tabela e colunas fornecidas; não use outras tabelas, nem DDL/DML."
    )
    user = (
        f"Tabela alvo: `{table_fqn}`.\\n"
        f"Colunas disponíveis:\\n{cols_txt}\\n\\n"
        f"Regras específicas:\\n"
        f"- Se a pergunta não trouxer período, filtre os últimos 90 dias usando a coluna `data_date`.\\n"
        f"- CTR = SAFE_DIVIDE(SUM(clicks), SUM(impressions)).\\n"
        f"- Posição média = SAFE_DIVIDE(SUM(sum_top_position), SUM(impressions)) AS position.\\n"
        f"- Para rankings, ordene por clicks ou impressions e limite resultados longos.\\n"
        f"- Comece diretamente com SELECT.\\n\\n"
        f"Pergunta do usuário:\\n{question}\\n"
    )
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{"role":"system","content":system},{"role":"user","content":user}],
        temperature=0.1,
    )
    return sanitize_sql(resp.choices[0].message.content.strip())

def ai_key_findings(question: str, df: pd.DataFrame, sql_used: str, n:int=6):
    if not client: return [{"title":"Configuração necessária","text":"Defina OPENAI_API."}]
    if df.empty:   return [{"title":"Sem dados","text":"Não há linhas para o recorte solicitado."}]
    preview = df.head(40).to_csv(index=False)
    system = (
        "Você é um analista de Marketing/SEO. Gere insights curtos e acionáveis "
        "com base nos dados fornecidos. Responda em JSON válido com a chave 'findings'. "
        "Não descreva SQL, não invente números; use apenas o que vier nos dados."
    )
    user = (
        f"Gere até {n} findings (curtos). Estrutura:\\n"
        f'{{"findings":[{{"title":"...", "text":"..."}}]}}\\n\\n'
        f"Pergunta do usuário:\\n{question}\\n\\n"
        f"SQL executada (contexto – não comente):\\n{sql_used}\\n\\n"
        f"Prévia dos resultados (CSV até 40 linhas):\\n{preview}"
    )
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{"role":"system","content":system},{"role":"user","content":user}],
        temperature=0.2,
        response_format={{"type":"json_object"}}
    )
    try:
        data = json.loads(resp.choices[0].message.content or "{{}}")
        findings = data.get("findings", [])
        out = []
        for it in findings[:n]:
            title = str(it.get("title","Insight")).strip()[:120]
            text  = str(it.get("text","")).strip()
            if text:
                out.append({{"title": title or "Insight", "text": text}})
        return out or [{{"title":"Sem insights","text":"Os dados retornados são muito curtos para gerar achados úteis."}}]
    except Exception:
        return [{{"title":"Resumo","text": resp.choices[0].message.content.strip()}}]

# --------- STATE ---------
if "insights" not in st.session_state:
    st.session_state.insights = []   # cada item: {q, findings, ts, sql}
if "pending" not in st.session_state:
    st.session_state.pending = None

# ================= UI =================
st.markdown("### Insights")

# bloco de controles (sticky)
with st.container():
    st.markdown('<div class="card panel-card sticky">', unsafe_allow_html=True)
    source = st.selectbox("Data source",
                          ["Google Search Console (BigQuery)", "Google Analytics 4 (em breve)", "Meta Ads (em breve)"],
                          index=0)
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    st.caption("Quick prompts")
    st.markdown('<div class="chips">', unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1: chip1 = st.button("Key findings for this period", key="chip1", use_container_width=True)
    with c2: chip2 = st.button("Compare with last month", key="chip2", use_container_width=True)
    c3, c4 = st.columns(2)
    with c3: chip3 = st.button("Top queries & pages", key="chip3", use_container_width=True)
    with c4: chip4 = st.button("Any anomalies?", key="chip4", use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    st.caption("Type your question")

    # textarea + botões lado a lado
    col_ta, col_btns = st.columns([0.67, 0.33])
    with col_ta:
        q = st.text_area(label=" ", label_visibility="collapsed",
                         height=90,
                         placeholder="e.g., Give me 5 actionable insights for this dataset and the selected period.")
    with col_btns:
        st.markdown('<div class="btn-row">', unsafe_allow_html=True)
        with st.container():
            st.markdown('<div class="btn-primary">', unsafe_allow_html=True)
            send = st.button("Send", use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
        with st.container():
            st.markdown('<div class="btn-secondary">', unsafe_allow_html=True)
            clear = st.button("Clear", use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)  # fecha panel-card

# chips → preenchem e enviam
if chip1: q, send = "Give me 5 key findings for the current period.", True
if chip2: q, send = "Summarize performance vs last month in up to 5 findings.", True
if chip3: q, send = "Top queries and pages driving the results this period.", True
if chip4: q, send = "Detect anomalies or significant day-to-day changes worth attention.", True

# limpar
if clear:
    st.session_state.insights = []
    st.session_state.pending = None
    st.rerun()

# enfileira
if send and q and q.strip():
    st.session_state.insights.insert(0, {"q": q.strip(), "findings": None, "ts": time.time(), "sql": None})
    st.session_state.pending = 0
    st.rerun()

# processa uma pendência
if st.session_state.pending is not None:
    idx = st.session_state.pending
    try:
        schema_cols = get_table_schema(BQ_TABLE) if bq else []
        sql = build_sql_with_ai(st.session_state.insights[idx]["q"], BQ_TABLE, schema_cols)
        if not sql or not sql_is_safe(sql):
            st.session_state.insights[idx]["findings"] = [{"title":"Consulta inválida","text":"Não foi possível gerar uma SQL segura. Refine a pergunta."}]
            st.session_state.insights[idx]["sql"] = sql or ""
        else:
            sql = ensure_limit(sql)
            df  = bq.query(sql).result().to_dataframe()
            findings = ai_key_findings(st.session_state.insights[idx]["q"], df, sql, n=6)
            st.session_state.insights[idx]["findings"] = findings
            st.session_state.insights[idx]["sql"] = sql
    except Exception as e:
        st.session_state.insights[idx]["findings"] = [{"title":"Erro ao consultar","text": str(e)}]
    finally:
        st.session_state.pending = None
        st.rerun()

# Key Findings (mais recente)
if st.session_state.insights:
    block = st.session_state.insights[0]
    st.markdown('<div class="card kf-card">', unsafe_allow_html=True)
    st.markdown('<div class="kf-title">Key Findings</div>', unsafe_allow_html=True)

    if block["findings"] is None:
        st.write("Gerando insights…")
    else:
        st.markdown('<ol class="kf-list">', unsafe_allow_html=True)
        for it in block["findings"]:
            title = html.escape(it.get("title","Insight"))
            text  = html.escape(it.get("text",""))
            st.markdown(f'<li><span class="kf-item-title">{title}</span><span class="kf-item-text">{text}</span></li>',
                        unsafe_allow_html=True)
        st.markdown('</ol>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    with st.expander("SQL usada (debug)"):
        st.code(block.get("sql") or "", language="sql")
else:
    st.info("Use os quick prompts ou escreva sua pergunta e clique em **Send** para gerar os insights.")
