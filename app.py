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
    st.error("Defina a variável de ambiente BQ_TABLE (ex.: projeto.dataset.tabela).")
if not SA_JSON:
    st.error("Defina GOOGLE_APPLICATION_CREDENTIALS_JSON com o conteúdo do JSON da Service Account.")
if not OPENAI_KEY:
    st.warning("Defina OPENAI_API para habilitar geração de SQL e respostas com IA.")

# Credencial em arquivo
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

# --------- STYLE (clean + compact mode) ---------
base_css = """
<style>
.main .block-container {max-width: 880px; padding-top: VAR_TOP;}
[data-testid="stChatMessage"] { margin: VAR_MSGMT; }
[data-testid="stChatMessage"] .stMarkdown { font-size: VAR_FONTSZ; line-height: VAR_LINEH; }
.user-bubble   { background: #0e3a2c; border: 1px solid #1d5e47; color: #e9fff5; padding: VAR_PAD; border-radius: 16px; }
.assist-bubble { background: #0c111a; border: 1px solid #233049; color: #dce6ff; padding: VAR_PAD; border-radius: 16px; }
.timechip { color:#8ea2c0; font-size: VAR_TSZ; margin-top: 4px; }
.ask-card {
  background: #0b0f16; border: 1px solid #233049; border-radius: 14px;
  padding: VAR_ASKPAD; box-shadow: 0 8px 20px rgba(0,0,0,.25); margin-bottom: 10px;
}
.btn-row { display:flex; gap:12px; align-items:center; }
</style>
"""
if COMPACT:
    css = (base_css
           .replace("VAR_TOP","0.4rem")
           .replace("VAR_MSGMT","6px 0 10px 0")
           .replace("VAR_FONTSZ",".90rem")
           .replace("VAR_LINEH","1.35rem")
           .replace("VAR_PAD","8px 12px")
           .replace("VAR_TSZ",".75rem")
           .replace("VAR_ASKPAD","10px 12px"))
else:
    css = (base_css
           .replace("VAR_TOP","0.9rem")
           .replace("VAR_MSGMT","8px 0 14px 0")
           .replace("VAR_FONTSZ",".95rem")
           .replace("VAR_LINEH","1.45rem")
           .replace("VAR_PAD","10px 14px")
           .replace("VAR_TSZ",".80rem")
           .replace("VAR_ASKPAD","14px 16px"))
st.markdown(css, unsafe_allow_html=True)

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

# ====== STATE ======
# thread: {"q": str, "a": Optional[str], "sql": Optional[str], "ts": float}
if "threads" not in st.session_state:
    st.session_state.threads = []
if "skip_first_render_once" not in st.session_state:
    st.session_state.skip_first_render_once = False

# ====== INPUT (topo, não some) ======
st.markdown('<div class="ask-card">', unsafe_allow_html=True)
colA, colB, colC = st.columns([6,1,1])
with colA:
    q = st.text_area("Faça sua pergunta:", height=80, key="ask_input",
                     placeholder="Ex.: Top 10 queries mobile no Brasil em agosto de 2024")
with colB:
    send = st.button("Enviar", use_container_width=True)
with colC:
    if st.button("Limpar", use_container_width=True):
        st.session_state.threads = []
        st.rerun()
st.markdown('</div>', unsafe_allow_html=True)

# ====== PROCESSA ENVIO (mostra bolhas instantâneas) ======
if send and q and q.strip():
    txt = q.strip()

    with st.chat_message("user", avatar=AVATAR_USER):
        st.markdown(f'<div class="user-bubble">{txt}</div>', unsafe_allow_html=True)

    with st.chat_message("assistant", avatar=AVATAR_BOT):
        ph = st.empty()
        with ph.container():
            st.markdown('<div class="assist-bubble">Analisando dados…</div>', unsafe_allow_html=True)

        # roda consulta
        try:
            schema_cols = get_table_schema(BQ_TABLE) if bq else []
            sql = build_sql_with_ai(txt, BQ_TABLE, schema_cols)
            if not sql or not sql_is_safe(sql):
                answer = "Não consegui gerar uma consulta segura para essa pergunta. Tente especificar período e/ou dimensões (ex.: mês, país, device)."
                df = pd.DataFrame()
            else:
                sql = ensure_limit(sql)
                df = bq.query(sql).result().to_dataframe()
                answer = ai_summary(txt, df, sql)

            # atualiza bolha
            with ph.container():
                st.markdown(f'<div class="assist-bubble">{answer}</div>', unsafe_allow_html=True)
                if (SHOW_SQL or SHOW_TABLE) and sql:
                    with st.expander("Detalhes da consulta"):
                        if SHOW_SQL:   st.code(sql, language="sql")
                        if SHOW_TABLE: st.dataframe(df, use_container_width=True)

            # salva thread (mais recente no topo) e sinaliza p/ não re-renderizar esta agora
            st.session_state.threads.insert(0, {"q": txt, "a": answer, "sql": sql, "ts": time.time()})
            st.session_state.skip_first_render_once = True

        except Exception as e:
            with ph.container():
                st.markdown(f'<div class="assist-bubble">Erro ao consultar: {e}</div>', unsafe_allow_html=True)

    # sobe para o topo
    st.markdown('<script>window.scrollTo(0,0);</script>', unsafe_allow_html=True)

# ====== RENDER HISTÓRICO (mais recente → antigo) ======
skip_first = st.session_state.skip_first_render_once
for i, th in enumerate(st.session_state.threads):
    if skip_first and i == 0:
        continue  # já mostramos a rodada corrente acima
    with st.chat_message("user", avatar=AVATAR_USER):
        st.markdown(f'<div class="user-bubble">{th["q"]}</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="timechip">{pd.to_datetime(th["ts"], unit="s").strftime("%Y-%m-%d %H:%M")}</div>', unsafe_allow_html=True)
    with st.chat_message("assistant", avatar=AVATAR_BOT):
        st.markdown(f'<div class="assist-bubble">{th["a"]}</div>', unsafe_allow_html=True)
        if (SHOW_SQL or SHOW_TABLE) and th.get("sql"):
            with st.expander("Detalhes da consulta"):
                if SHOW_SQL:   st.code(th["sql"], language="sql")
                if SHOW_TABLE:
                    try:
                        df_prev = bq.query(th["sql"]).result().to_dataframe(max_results=300)
                        st.dataframe(df_prev, use_container_width=True)
                    except Exception as e:
                        st.write(f"Falha ao carregar amostra: {e}")

# limpa o flag para o próximo ciclo
if st.session_state.skip_first_render_once:
    st.session_state.skip_first_render_once = False
