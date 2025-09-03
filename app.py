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
COMPACT      = os.getenv("COMPACT", "0").strip() == "1"
AVATAR_USER  = os.getenv("AVATAR_USER", "🧑‍💻")
AVATAR_BOT   = os.getenv("AVATAR_BOT", "🤖")

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

# --------- OpenAI (sem proxies do ambiente) ---------
from openai import OpenAI
import httpx
client = None
if OPENAI_KEY:
    http_client = httpx.Client(timeout=60.0, follow_redirects=True, trust_env=False)
    client = OpenAI(api_key=OPENAI_KEY, http_client=http_client)

# --------- STYLE (profissional + compacto) ---------
base_css = """
<style>
:root{ --radius:16px; --card:#0c111a; --border:#233049; --muted:#8ea2c0;
       --qbg:#11202e; --qtxt:#e9f4ff; --atxt:#dce6ff; }
.main .block-container {max-width: 900px; padding-top: VAR_TOP;}
/* cartão Q&A único (pergunta+resposta) */
.qa-block{ background:var(--card); border:1px solid var(--border); border-radius:var(--radius);
           padding: VAR_PAD; margin: 12px 0; box-shadow: 0 8px 24px rgba(0,0,0,.25); }
.qa-head{ display:flex; align-items:center; gap:10px; margin-bottom:10px; }
.qa-head .avatar{font-size:1.1rem}
.qa-head .label{color:var(--muted); font-size:VAR_TSZ}
.qa-q{ background:var(--qbg); color:var(--qtxt); padding: VAR_QPAD; border-radius:12px;
       font-weight:600; margin-bottom:12px; }
.qa-a{ color:var(--atxt); font-size: VAR_FSZ; line-height: VAR_LH; }
.qa-time{ color:var(--muted); font-size:VAR_TSZ; margin-left:auto; }
/* barra de pergunta no topo */
.ask-card{ background:#0b0f16; border:1px solid var(--border); border-radius:14px;
           padding: VAR_APAD; box-shadow: 0 8px 20px rgba(0,0,0,.25); margin-bottom: 10px; }
/* input + enviar na mesma linha */
.send-wrap button{
  height: VAR_BTNH; border-radius: 10px; padding: 0 14px; border:1px solid var(--border);
}
.send-wrap button:hover{ filter:brightness(1.06); }
/* botão limpar minimalista (link) */
.clear-wrap{ display:flex; justify-content:flex-end; margin-top:6px; }
.clear-wrap button{
  background:transparent; border:0; color:var(--muted); padding:0; margin:0;
  text-decoration: underline; cursor:pointer; font-size: VAR_TSZ;
}
.clear-wrap button:hover{ color:#cbd6ea; }
</style>
"""
if COMPACT:
    css = (base_css
           .replace("VAR_TOP","0.5rem")
           .replace("VAR_PAD","12px 14px")
           .replace("VAR_QPAD","8px 10px")
           .replace("VAR_FSZ",".92rem")
           .replace("VAR_LH","1.38rem")
           .replace("VAR_TSZ",".76rem")
           .replace("VAR_APAD","10px 12px")
           .replace("VAR_BTNH","42px"))
else:
    css = (base_css
           .replace("VAR_TOP","0.9rem")
           .replace("VAR_PAD","16px 18px")
           .replace("VAR_QPAD","10px 12px")
           .replace("VAR_FSZ",".98rem")
           .replace("VAR_LH","1.45rem")
           .replace("VAR_TSZ",".82rem")
           .replace("VAR_APAD","14px 16px")
           .replace("VAR_BTNH","46px"))
st.markdown(css, unsafe_allow_html=True)

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
# Cada thread: {"q": str, "a": Optional[str], "sql": Optional[str], "ts": float, "df_sample": Optional[list], "df_cols": Optional[list]}
if "threads" not in st.session_state:
    st.session_state.threads = []
if "pending_index" not in st.session_state:
    st.session_state.pending_index = None

# --------- INPUT (topo) — sem label, enviar ao lado, limpar minimalista ---------
st.markdown('<div class="ask-card">', unsafe_allow_html=True)
col_input, col_send = st.columns([7.5, 1])
with col_input:
    question_text = st.text_area(
        label=" ",
        label_visibility="collapsed",
        height=COMPACT and 66 or 80,
        placeholder="Ex.: Performance orgânica de agosto de 2025 para mobile"
    )
with col_send:
    st.markdown('<div class="send-wrap">', unsafe_allow_html=True)
    send = st.button("Enviar", use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

# botão limpar minimalista alinhado à direita
st.markdown('<div class="clear-wrap">', unsafe_allow_html=True)
clear = st.button("Limpar", key="clear_btn")
st.markdown('</div>', unsafe_allow_html=True)
st.markdown('</div>', unsafe_allow_html=True)  # fecha ask-card

if clear:
    st.session_state.threads = []
    st.session_state.pending_index = None
    st.rerun()

# Ao enviar: cria thread (sem resposta) e agenda processamento 1x
if send and question_text and question_text.strip():
    st.session_state.threads.insert(0, {
        "q": question_text.strip(), "a": None, "sql": None,
        "ts": time.time(), "df_sample": None, "df_cols": None
    })
    st.session_state.pending_index = 0
    st.rerun()

# Processa UMA thread pendente, salva resposta e não reexecuta as antigas
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

    # Cabeçalho
    st.markdown(
        f'<div class="qa-head"><span class="avatar">{AVATAR_USER}</span>'
        f'<span class="label">Pergunta</span>'
        f'<span class="qa-time">{ts_txt}</span></div>',
        unsafe_allow_html=True
    )
    # Pergunta + Resposta (no mesmo bloco)
    st.markdown(f'<div class="qa-q">{th["q"]}</div>', unsafe_allow_html=True)
    st.markdown(
        f'<div class="qa-head" style="margin-top:6px;"><span class="avatar">{AVATAR_BOT}</span>'
        f'<span class="label">Análise</span></div>',
        unsafe_allow_html=True
    )
    st.markdown(f'<div class="qa-a">{th["a"] or "Processando…"}</div>', unsafe_allow_html=True)

    # Detalhes opcionais
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
