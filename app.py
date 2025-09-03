import os, re, time
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

st.set_page_config(page_title="GSC → BigQuery: Chat de Dados", layout="wide")

# ====== STYLE (clean) ======
st.markdown("""
<style>
/* página mais clean */
.main .block-container {padding-top: 1.2rem; max-width: 980px;}
/* título menor */
h3 { margin-top: .2rem; margin-bottom: .6rem; }
/* input form */
.form-card {
  background: #0e1116;
  border: 1px solid #283043;
  border-radius: 14px;
  padding: 14px 16px;
  box-shadow: 0 6px 18px rgba(0,0,0,.25);
}
/* cards de Q&A */
.qa-card {
  background: #0b0e13;
  border: 1px solid #233049;
  border-radius: 16px;
  padding: 14px 16px;
  margin: 12px 0;
  box-shadow: 0 8px 24px rgba(0,0,0,.28);
}
.qa-header { display:flex; align-items:center; justify-content:space-between; margin-bottom:8px; }
.qa-tag {
  font-size:.80rem; padding:2px 8px; border-radius:999px; border:1px solid #2c3b57; color:#aab6d1;
}
.qa-q { font-weight:600; font-size:1rem; margin: 4px 0 8px 0; }
.qa-a { font-size:.95rem; line-height:1.45rem; }
.muted { color:#8c96aa; font-size:.85rem; }
.hr { height:1px; background:#1e2535; margin:10px 0; border-radius:1px; }
.small-btn { font-size:.82rem; padding:4px 8px; border:1px solid #2b3954; border-radius:8px; background:#101521; color:#c7d1e6; cursor:pointer;}
.small-btn:hover { filter:brightness(1.06); }
.clear-btn { float:right; margin-top:-28px; }
</style>
""", unsafe_allow_html=True)

# --------- ENV VARS ---------
BQ_TABLE     = os.getenv("BQ_TABLE", "").strip()  # ex: project.dataset.table
SA_JSON      = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON", "").strip()
OPENAI_KEY   = os.getenv("OPENAI_API", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()
SHOW_SQL     = os.getenv("SHOW_SQL", "0").strip() == "1"
SHOW_TABLE   = os.getenv("SHOW_TABLE", "0").strip() == "1"

if not BQ_TABLE:
    st.error("Defina a variável de ambiente BQ_TABLE (ex.: projeto.dataset.tabela).")
if not SA_JSON:
    st.error("Defina GOOGLE_APPLICATION_CREDENTIALS_JSON com o conteúdo do JSON da Service Account.")
if not OPENAI_KEY:
    st.warning("Defina OPENAI_API para habilitar geração de SQL e respostas com IA.")

# Escreve a credencial em arquivo (padrão do SDK GCP)
if SA_JSON:
    SA_PATH = "/tmp/sa.json"
    with open(SA_PATH, "w") as f:
        f.write(SA_JSON)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_PATH

# --------- BIGQUERY ---------
from google.cloud import bigquery
@st.cache_resource(show_spinner=False)
def get_bq(): return bigquery.Client()
bq = get_bq() if SA_JSON else None

@st.cache_data(show_spinner=False)
def get_table_schema(table_fqn: str):
    tbl = bq.get_table(table_fqn)
    return [(s.name, s.field_type) for s in tbl.schema]

# --------- OPENAI (sem proxies) ---------
from openai import OpenAI
import httpx
client = None
if OPENAI_KEY:
    http_client = httpx.Client(timeout=60.0, follow_redirects=True, trust_env=False)
    client = OpenAI(api_key=OPENAI_KEY, http_client=http_client)

# --------- HELPERS: SQL ---------
def sanitize_sql(text: str) -> str:
    if not text: return ""
    t = text.strip()
    t = re.sub(r"^sql\s*", "", t, flags=re.IGNORECASE)
    t = re.sub(r"^```(?:sql)?\s*|\s*```$", "", t, flags=re.IGNORECASE | re.DOTALL)
    m = re.search(r"\bselect\b", t, flags=re.IGNORECASE)
    if m: t = t[m.start():]
    return t.strip().rstrip(";")

def sql_is_safe(sql: str) -> bool:
    s = sql.strip(); s_lower = s.lower()
    if not re.match(r"^\s*select\b", s_lower): return False
    forbidden = ["insert","update","delete","merge","drop","create","alter","truncate",";","--","/*"]
    if any(tok in s_lower for tok in forbidden): return False
    target_clean = re.sub(r"[`\s]", "", BQ_TABLE.lower())
    s_clean = re.sub(r"[`\s]", "", s_lower)
    return target_clean in s_clean

def ensure_limit(sql: str, default_limit: int = 1000) -> str:
    return sql if re.search(r"\blimit\b\s+\d+\s*$", sql, re.I) else f"{sql}\nLIMIT {default_limit}"

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

def ai_summary(question: str, df: pd.DataFrame, sql_used: str) -> str:
    if not client: return "Defina OPENAI_API para habilitar a síntese de respostas."
    if df.empty:   return "Sem dados para o recorte solicitado."
    preview = df.head(30).to_csv(index=False)
    system = (
        "Você é um analista de SEO focado em dados do Google Search Console no BigQuery. "
        "Responda APENAS com análise baseada nos dados retornados. "
        "Nunca sugira SQL, não crie 'resultados esperados' e não invente números. "
        "Formate em 3–6 bullets objetivos, citando números reais (clicks, impressions, CTR, position) quando fizer sentido."
    )
    user = (
        f"Pergunta do usuário:\n{question}\n\n"
        f"SQL executada (apenas contexto, não comente sobre ela):\n{sql_used}\n\n"
        f"Prévia dos resultados (até 30 linhas em CSV):\n{preview}"
    )
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{"role":"system","content":system},{"role":"user","content":user}],
        temperature=0.2,
    )
    return resp.choices[0].message.content.strip()

# ====== STATE: threads (Q&A) ======
# cada thread: {"q": str, "a": Optional[str], "sql": Optional[str], "ts": float}
if "threads" not in st.session_state:
    st.session_state.threads = []

# ====== FORM (topo) ======
with st.container():
    st.markdown('<div class="form-card">', unsafe_allow_html=True)
    with st.form("chat_form", clear_on_submit=True):
        q = st.text_area("Faça sua pergunta:", placeholder="Ex.: Top 10 queries mobile no Brasil em agosto de 2024", height=80)
        c1, c2 = st.columns([1,1])
        with c1:
            submitted = st.form_submit_button("Enviar")
        with c2:
            if st.form_submit_button("Limpar histórico"):
                st.session_state.threads = []
                st.experimental_rerun()
    st.markdown('</div>', unsafe_allow_html=True)

# ao enviar, cria thread (no topo) e já processa
if submitted and q:
    st.session_state.threads.insert(0, {"q": q.strip(), "a": None, "sql": None, "ts": time.time()})
    # processa imediatamente a nova pergunta
    try:
        schema_cols = get_table_schema(BQ_TABLE) if bq else []
        sql = build_sql_with_ai(q, BQ_TABLE, schema_cols)
        if not sql or not sql_is_safe(sql):
            a = "Não consegui gerar uma consulta segura para essa pergunta. Tente especificar período e/ou dimensões."
        else:
            sql = ensure_limit(sql)
            df = bq.query(sql).result().to_dataframe()
            a  = ai_summary(q, df, sql)
        st.session_state.threads[0]["a"] = a
        st.session_state.threads[0]["sql"] = sql
    except Exception as e:
        st.session_state.threads[0]["a"] = f"Erro ao consultar: {e}"
        st.session_state.threads[0]["sql"] = None
    # reposiciona no topo e sobe a página
    st.markdown('<script>window.scrollTo(0,0);</script>', unsafe_allow_html=True)

# ====== RENDER (mais recente primeiro) ======
for th in st.session_state.threads:
    st.markdown('<div class="qa-card">', unsafe_allow_html=True)
    st.markdown(
        f'<div class="qa-header"><span class="qa-tag">Pergunta</span>'
        f'<span class="muted">{pd.to_datetime(th["ts"], unit="s").strftime("%Y-%m-%d %H:%M")}</span></div>',
        unsafe_allow_html=True
    )
    st.markdown(f'<div class="qa-q">{th["q"]}</div>', unsafe_allow_html=True)
    st.markdown('<div class="hr"></div>', unsafe_allow_html=True)
    st.markdown('<div class="qa-header"><span class="qa-tag">Análise</span></div>', unsafe_allow_html=True)
    st.markdown(f'<div class="qa-a">{th["a"] or "Processando..."}</div>', unsafe_allow_html=True)

    # opcionais para debug/analise
    if SHOW_TABLE or SHOW_SQL:
        with st.expander("Detalhes da consulta"):
            if SHOW_SQL and th.get("sql"):
                st.code(th["sql"], language="sql")
            if SHOW_TABLE and th.get("sql"):
                try:
                    df_prev = bq.query(th["sql"]).result().to_dataframe(max_results=300)
                    st.dataframe(df_prev, use_container_width=True)
                except Exception as e:
                    st.write(f"Falha ao carregar amostra: {e}")
    st.markdown('</div>', unsafe_allow_html=True)

# garante que a página mantém o topo visível (novo primeiro)
st.components.v1.html("<script>window.scrollTo(0,0);</script>", height=0)
