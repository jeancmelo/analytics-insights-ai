import os, re, json, time
from datetime import date, timedelta
import pandas as pd
import streamlit as st
from html import escape

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
BQ_TABLE     = os.getenv("BQ_TABLE", "").strip()         # ex: project.dataset.table
SA_JSON      = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON", "").strip()
OPENAI_KEY   = os.getenv("OPENAI_API", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

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

# --------- OpenAI (sem herdar proxies) ---------
from openai import OpenAI
import httpx
client = None
if OPENAI_KEY:
    http_client = httpx.Client(timeout=60.0, follow_redirects=True, trust_env=False)
    client = OpenAI(api_key=OPENAI_KEY, http_client=http_client)

# --------- STYLE (tema claro, painel estreito como no print) ---------
st.markdown("""
<style>
/* (1) QUICK PROMPTS: botões claros, como o textarea */
[data-testid="stAppViewContainer"] .chips .stButton > button {
  background: #f8fafc !important;          /* claro */
  background-color: #f8fafc !important;
  color: #111827 !important;                /* texto preto */
  border: 1px solid #e5e7eb !important;     /* borda clara */
  box-shadow: none !important;
}
[data-testid="stAppViewContainer"] .chips .stButton > button:hover {
  background: #f1f5f9 !important;
  background-color: #f1f5f9 !important;
  border-color: #cbd5e1 !important;
}
/* garante texto preto mesmo se o tema envolver dentro de <p>/<span> */
.chips .stButton > button p,
.chips .stButton > button span { color:#111827 !important; }

/* (2) LABEL “Type your question”: tom cinza escuro para aparecer */
[data-testid="stCaption"] { color:#374151 !important; }  /* afeta 'Data source', 'Quick prompts' e 'Type your question' */

/* (3) Diminuir o espaçamento entre SEND e CLEAR (igual aos chips: 8px) */
.btn-row { display:grid !important; grid-template-columns: 1fr !important; gap:8px !important; }
.btn-row .stButton { margin:0 !important; }  /* remove margens extras do Streamlit */

[class^="st-emotion-cache-"] { gap: 0 !important; row-gap: 5px !important;}

/* combinado (cobre ambos os casos) */
li[class^="st-emotion-cache-"],
li[class*=" st-emotion-cache-"] { margin-bottom: 6% !important; padding: 0px 0px 0px 0.6em !important;}

.block-container  {padding: 3rem 1rem 10rem  !important;}
/* Key Findings – lista numerada elegante */
.kf-list { counter-reset:item; list-style:none; padding-left:0; margin:0; }
.kf-list li { counter-increment:item; margin:.55rem 0; }
.kf-list li::before { content: counter(item) "."; font-weight:700; margin-right:.35rem; color:#111827; }
.kf-item-title { font-weight:700; }
.kf-item-text { display:block; margin-top:.15rem; }
.kf-title { font-weight:700; margin-bottom: 5%;}

/* divisória */
.divider{{ height:1px; background:#e5e7eb; margin:.6rem 0; }}

</style>
""", unsafe_allow_html=True)

# --------- Helpers: SQL ---------
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

# --------- LLM prompts (gera SQL e depois findings em JSON) ---------
def build_sql_with_ai(question: str, table_fqn: str, columns: list) -> str:
    if not client: return ""
    cols_txt = "\n".join([f"- {c} ({t})" for c, t in columns])
    system = (
        "Você é um gerador de SQL para BigQuery. "
        "Responda SOMENTE com a consulta SQL (sem rótulos, sem explicações, sem cercas de código). "
        "Use exclusivamente a tabela e colunas fornecidas; não use outras tabelas, nem DDL/DML."
    )
    user = (
        f"Tabela alvo: `{table_fqn}`.\n"
        f"Colunas disponíveis:\n{cols_txt}\n\n"
        f"Regras específicas:\n"
        f"- Se a pergunta não trouxer período, filtre os últimos 90 dias usando a coluna `data_date`.\n"
        f"- CTR = SAFE_DIVIDE(SUM(clicks), SUM(impressions)).\n"
        f"- Posição média = SAFE_DIVIDE(SUM(sum_top_position), SUM(impressions)) AS position.\n"
        f"- Para rankings, ordene por clicks ou impressions e limite resultados longos.\n"
        f"- Comece diretamente com SELECT.\n\n"
        f"Pergunta do usuário:\n{question}\n"
    )
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{"role":"system","content":system},{"role":"user","content":user}],
        temperature=0.1,
    )
    return sanitize_sql(resp.choices[0].message.content.strip())

def ai_key_findings(question: str, df: pd.DataFrame, sql_used: str, n:int=5):
    """Pede findings em JSON: {"findings":[{"title":...,"text":...}]}"""
    if not client: return [{"title":"Configuração necessária","text":"Defina OPENAI_API."}]
    if df.empty:   return [{"title":"Sem dados","text":"Não há linhas para o recorte solicitado."}]
    preview = df.head(40).to_csv(index=False)
    system = (
        "Você é um analista de Marketing/SEO. Gere insights curtos e acionáveis "
        "com base nos dados fornecidos. Responda em JSON válido com a chave 'findings'. "
        "Não descreva SQL, não invente números; use apenas o que vier nos dados."
    )
    user = (
        f"Gere até {n} findings (curtos). Estrutura:\n"
        f'{{"findings":[{{"title":"...", "text":"..."}}]}}\n\n'
        f"Pergunta do usuário:\n{question}\n\n"
        f"SQL executada (contexto – não comente):\n{sql_used}\n\n"
        f"Prévia dos resultados (CSV até 40 linhas):\n{preview}"
    )
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{"role":"system","content":system},{"role":"user","content":user}],
        temperature=0.2,
        response_format={"type":"json_object"}
    )
    try:
        data = json.loads(resp.choices[0].message.content or "{}")
        findings = data.get("findings", [])
        # saneamento simples
        out = []
        for it in findings[:n]:
            title = str(it.get("title","Insight")).strip()[:120]
            text  = str(it.get("text","")).strip()
            if text:
                out.append({"title":title or "Insight", "text":text})
        return out or [{"title":"Sem insights","text":"Os dados retornados são muito curtos para gerar achados úteis."}]
    except Exception:
        # fallback: tudo em um finding único
        return [{"title":"Resumo", "text": resp.choices[0].message.content.strip()}]

# --------- STATE ---------
if "insights" not in st.session_state:
    st.session_state.insights = []   # lista de blocos: {q:str, findings:[{title,text}], ts:float, sql:str}
if "pending" not in st.session_state:
    st.session_state.pending = None  # índice do insight a processar

# --------- UI: Header + fonte de dados ---------
st.markdown("### Generative Insights")
with st.container():
    st.markdown('<div class="panel-card">', unsafe_allow_html=True)
    source = st.selectbox("Data source", ["Google Search Console (BigQuery)", "Google Analytics 4 (em breve)", "Meta Ads (em breve)"], index=0)
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    # Quick prompts (chips)
    st.caption("Quick prompts")
    c1, c2 = st.columns(2)
    with c1:
        chip1 = st.button("Key findings for this period", key="chip1")
    with c2:
        chip2 = st.button("Compare with last month", key="chip2")
    c3, c4 = st.columns(2)
    with c3:
        chip3 = st.button("Top queries & pages", key="chip3")
    with c4:
        chip4 = st.button("Any anomalies to highlight?", key="chip4")
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    # Pergunta + botões (enviar/limpar)
    st.caption("Type your question")
    col_input, col_btns = st.columns([0.7, 0.3])
    with col_input:
        st.markdown('<div class="textarea">', unsafe_allow_html=True)
        q = st.text_area(label=" ", label_visibility="collapsed", key="ask", height=90,
                         placeholder="e.g., Give me 5 actionable insights for this dataset and the selected period.")
        st.markdown('</div>', unsafe_allow_html=True)
    with col_btns:
        st.markdown('<div class="btn-primary">', unsafe_allow_html=True)
        send = st.button("Send", use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('<div class="btn-secondary">', unsafe_allow_html=True)
        clear = st.button("Clear insights", use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)  # fecha panel-card

# Chips preenchem e enviam
if chip1: q, send = "Give me 5 key findings for the current period.", True
if chip2: q, send = "Summarize performance vs last month in up to 5 findings.", True
if chip3: q, send = "Top queries and pages driving the results this period.", True
if chip4: q, send = "Detect anomalies or significant day-to-day changes worth attention.", True

# Limpar
if clear:
    st.session_state.insights = []
    st.session_state.pending = None
    st.rerun()

# Enfileira um insight para processar
if send and q and q.strip():
    st.session_state.insights.insert(0, {"q": q.strip(), "findings": None, "ts": time.time(), "sql": None})
    st.session_state.pending = 0
    st.rerun()

# Processa UMA pendência
if st.session_state.pending is not None:
    idx = st.session_state.pending
    try:
        schema_cols = get_table_schema(BQ_TABLE) if bq else []
        sql = build_sql_with_ai(st.session_state.insights[idx]["q"], BQ_TABLE, schema_cols)
        if not sql or not sql_is_safe(sql):
            st.session_state.insights[idx]["findings"] = [{"title":"Consulta inválida","text":"Não foi possível gerar uma SQL segura. Refaça a pergunta especificando período/dimensões."}]
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

# --------- Render: Key Findings (mais recente) ---------
if st.session_state.insights:
    block = st.session_state.insights[0]
    st.markdown('<div class="card kf-card">', unsafe_allow_html=True)
    st.markdown('<div class="kf-title">Key Findings</div>', unsafe_allow_html=True)

    if block["findings"] is None:
        st.write("Gerando insights…")
    else:
        st.markdown('<ol class="kf-list">', unsafe_allow_html=True)
        for it in block["findings"]:
            title = escape(str(it.get("title","Insight")))
            text  = escape(str(it.get("text","")))

            st.markdown(
                f'<li><span class="kf-item-title">{title}</span>'
                f'<span class="kf-item-text">{text}</span></li>',
                unsafe_allow_html=True
            )
        st.markdown('</ol>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)


    # opcional: mostrar SQL usada
    with st.expander("SQL usada (debug)"):
        st.code(block.get("sql") or "", language="sql")
else:
    # estado vazio amigável
    st.info("Use os quick prompts acima ou escreva sua pergunta e clique em **Send** para gerar os insights.")
