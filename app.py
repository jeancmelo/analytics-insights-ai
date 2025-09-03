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

# --------- ENV VARS ---------
BQ_TABLE     = os.getenv("BQ_TABLE", "").strip()
SA_JSON      = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON", "").strip()
OPENAI_KEY   = os.getenv("OPENAI_API", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()
SHOW_SQL     = os.getenv("SHOW_SQL", "0").strip() == "1"
SHOW_TABLE   = os.getenv("SHOW_TABLE", "0").strip() == "1"
AVATAR_USER  = os.getenv("AVATAR_USER", "🙂")
AVATAR_BOT   = os.getenv("AVATAR_BOT", "📈")

if not BQ_TABLE:
    st.error("Defina BQ_TABLE (ex.: projeto.dataset.tabela).")
if not SA_JSON:
    st.error("Defina GOOGLE_APPLICATION_CREDENTIALS_JSON com o conteúdo do JSON da Service Account.")
if not OPENAI_KEY:
    st.warning("Defina OPENAI_API para habilitar a IA.")

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

# --------- OpenAI (sem proxies) ---------
from openai import OpenAI
import httpx
client = None
if OPENAI_KEY:
    http_client = httpx.Client(timeout=60.0, follow_redirects=True, trust_env=False)
    client = OpenAI(api_key=OPENAI_KEY, http_client=http_client)

# --------- STYLE (tema claro, profissional) ---------
st.markdown("""
<style>
/* fundo branco geral para casar com Looker Studio */
html, body, .stApp, [data-testid="stAppViewContainer"], .main { background: #ffffff !important; color:#0f172a; }
.main .block-container {max-width: 980px; padding-top: .8rem;}
/* cartão Q&A único (pergunta+resposta) */
.qa-block{
  background:#ffffff; border:1px solid #e5e7eb; border-radius:16px;
  padding:16px 18px; margin: 14px 0; box-shadow: 0 6px 18px rgba(31,41,55,.08);
}
.qa-head{ display:flex; align-items:center; gap:.5rem; margin-bottom:8px; }
.qa-head .avatar{font-size:1.05rem}
.qa-head .label{color:#6b7280; font-size:.82rem}
.qa-time{ color:#6b7280; font-size:.8rem; margin-left:auto; }
.qa-q{
  background:#eef2ff; color:#111827; padding:10px 12px; border-radius:12px;
  font-weight:600; margin-bottom:10px;
}
.qa-a{ color:#0f172a; font-size:.98rem; line-height:1.5rem; }
/* barra de pergunta no topo */
.ask-card{
  background:#ffffff; border:1px solid #e5e7eb; border-radius:14px;
  padding:12px 14px; box-shadow: 0 6px 18px rgba(31,41,55,.08); margin-bottom: 12px;
}
/* input + enviar na mesma linha */
.send-wrap { display:flex; gap:10px; align-items:stretch; }
.send-wrap textarea{ min-height:56px; }
.send-wrap .stButton>button{
  height:56px; align-self:stretch; border-radius:10px; padding:0 18px;
  border:1px solid #e5e7eb; background:#111827; color:#fff;
}
.send-wrap .stButton>button:hover{ filter:brightness(1.05); }
/* botão limpar minimalista (link) alinhado à direita */
.clear-wrap{ display:flex; justify-content:flex-end; margin-top:6px; }
.clear-wrap .stButton>button{
  background:transparent; border:0; color:#6b7280; padding:0; margin:0;
  text-decoration: underline; cursor:pointer; font-size:.82rem;
}
.clear-wrap .stButton>button:hover{ color:#111827; }
/* textarea borda clara */
.stTextArea>div>div textarea { border:1px solid #e5e7eb; border-radius:10px; }
</style>
""", unsafe_allow_html=True)

# --------- Helpers: SQL ---------
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
        f"- Ordene rankings por clicks ou impressions; limite resultados longos.\n"
        f"- Comece diretamente com SELECT.\n\n"
        f"Pergunta do usuário:\n{question}\n"
    )
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{"role":"system","content":system},{"role":"user","content":user}],
        temperature=0.1,
    )
    return sanitize_sql(resp.choices[0].message.content.strip())

def ai_summary_paragraph(question: str, df: pd.DataFrame, sql_used: str) -> str:
    if not client: return "Defina OPENAI_API para habilitar a síntese de respostas."
    if df.empty:   return "Sem dados para o recorte solicitado."
    preview = df.head(25).to_csv(index=False)
    system = (
        "Você é um analista de SEO especializado em Search Console no BigQuery. "
        "Escreva a resposta em tom profissional e humano, em 1–2 parágrafos curtos. "
        "Baseie-se apenas nos dados fornecidos; traga números quando forem relevantes. "
        "Não liste itens, não crie tópicos, não descreva SQL, não invente valores."
    )
    user = (
        f"Pergunta do usuário:\n{question}\n\n"
        f"SQL executada (apenas contexto, não comente sobre ela):\n{sql_used}\n\n"
        f"Prévia dos resultados (até 25 linhas em CSV):\n{preview}"
    )
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{"role":"system","content":system},{"role":"user","content":user}],
        temperature=0.2,
    )
    return resp.choices[0].message.content.strip()

# --------- STATE ---------
# thread: {"q":str,"a":str|None,"sql":str|None,"ts":float,"df_sample":list|None,"df_cols":list|None}
if "threads" not in st.session_state:
    st.session_state.threads = []
if "pending_index" not in st.session_state:
    st.session_state.pending_index = None

# --------- INPUT (topo): Enviar ao lado, Limpar link à direita ---------
st.markdown('<div class="ask-card">', unsafe_allow_html=True)
st.markdown('<div class="send-wrap">', unsafe_allow_html=True)
col_input, col_send = st.columns([8, 1.6])
with col_input:
    question_text = st.text_area(
        label=" ",
        label_visibility="collapsed",
        height=56,
        placeholder="Ex.: Performance orgânica de agosto de 2025 para mobile"
    )
with col_send:
    send = st.button("Enviar", use_container_width=True)
st.markdown('</div>', unsafe_allow_html=True)

st.markdown('<div class="clear-wrap">', unsafe_allow_html=True)
clear = st.button("Limpar", key="clear_btn")
st.markdown('</div>', unsafe_allow_html=True)
st.markdown('</div>', unsafe_allow_html=True)

if clear:
    st.session_state.threads = []
    st.session_state.pending_index = None
    st.rerun()

# Ao enviar: cria thread e agenda processamento 1x
if send and question_text and question_text.strip():
    st.session_state.threads.insert(0, {
        "q": question_text.strip(), "a": None, "sql": None,
        "ts": time.time(), "df_sample": None, "df_cols": None
    })
    st.session_state.pending_index = 0
    st.rerun()

# Processa UMA pendência e salva
if st.session_state.pending_index is not None:
    try:
        th = st.session_state.threads[st.session_state.pending_index]
        schema_cols = get_table_schema(BQ_TABLE) if bq else []
        sql = build_sql_with_ai(th["q"], BQ_TABLE, schema_cols)
        if not sql or not sql_is_safe(sql):
            answer = "Não foi possível gerar uma consulta segura para essa pergunta. Tente especificar período e/ou dimensões (meses, país, device)."
            df = pd.DataFrame()
        else:
            sql = ensure_limit(sql)
            df = bq.query(sql).result().to_dataframe()
            answer = ai_summary_paragraph(th["q"], df, sql)
        th["a"] = answer
        th["sql"] = sql
        if not df.empty:
            th["df_cols"] = list(df.columns)
            th["df_sample"] = df.head(300).to_dict(orient="records")
    except Exception as e:
        st.session_state.threads[st.session_state.pending_index]["a"] = f"Erro ao consultar: {e}"
        st.session_state.threads[st.session_state.pending_index]["sql"] = None
    finally:
        st.session_state.pending_index = None
        st.rerun()

# --------- RENDER (mais recente → antigo) ---------
for th in st.session_state.threads:
    ts_txt = pd.to_datetime(th["ts"], unit="s").strftime("%Y-%m-%d %H:%M")
    st.markdown('<div class="qa-block">', unsafe_allow_html=True)
    st.markdown(
        f'<div class="qa-head"><span class="avatar">{AVATAR_USER}</span>'
        f'<span class="label">Pergunta</span>'
        f'<span class="qa-time">{ts_txt}</span></div>', unsafe_allow_html=True
    )
    st.markdown(f'<div class="qa-q">{th["q"]}</div>', unsafe_allow_html=True)
    st.markdown(
        f'<div class="qa-head" style="margin-top:6px;"><span class="avatar">{AVATAR_BOT}</span>'
        f'<span class="label">Análise</span></div>', unsafe_allow_html=True
    )
    st.markdown(f'<div class="qa-a">{th["a"] or "Processando…"}</div>', unsafe_allow_html=True)

    if (SHOW_SQL or SHOW_TABLE) and th.get("sql"):
        with st.expander("Detalhes da consulta"):
            if SHOW_SQL: st.code(th["sql"], language="sql")
            if SHOW_TABLE and th.get("df_sample"):
                try:
                    df_prev = pd.DataFrame(th["df_sample"], columns=th["df_cols"])
                    st.dataframe(df_prev, use_container_width=True)
                except Exception as e:
                    st.write(f"Falha ao exibir amostra: {e}")
    st.markdown('</div>', unsafe_allow_html=True)
