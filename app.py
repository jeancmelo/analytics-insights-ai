import os, re
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
BQ_TABLE   = os.getenv("BQ_TABLE", "").strip()  # ex: project.dataset.table
SA_JSON    = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON", "").strip()
OPENAI_KEY = os.getenv("OPENAI_API", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

SHOW_SQL   = os.getenv("SHOW_SQL", "0").strip() == "1"     # expander opcional
SHOW_TABLE = os.getenv("SHOW_TABLE", "0").strip() == "1"   # expander opcional

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

# --------- BIGQUERY CLIENT ---------
from google.cloud import bigquery
@st.cache_resource(show_spinner=False)
def get_bq():
    return bigquery.Client()
bq = None
if SA_JSON:
    try:
        bq = get_bq()
    except Exception as e:
        st.error(f"Erro ao iniciar BigQuery Client: {e}")

@st.cache_data(show_spinner=False)
def get_table_schema(table_fqn: str):
    tbl = bq.get_table(table_fqn)
    return [(s.name, s.field_type) for s in tbl.schema]

# --------- OPENAI CLIENT (sem herdar proxies) ---------
from openai import OpenAI
import httpx
client = None
if OPENAI_KEY:
    http_client = httpx.Client(timeout=60.0, follow_redirects=True, trust_env=False)
    client = OpenAI(api_key=OPENAI_KEY, http_client=http_client)

# --------- HELPERS ---------
def sanitize_sql(text: str) -> str:
    """Remove 'sql', cercas ```sql e recorta a partir do primeiro SELECT; remove ';' final."""
    if not text:
        return ""
    t = text.strip()
    t = re.sub(r"^sql\s*", "", t, flags=re.IGNORECASE)
    t = re.sub(r"^```(?:sql)?\s*|\s*```$", "", t, flags=re.IGNORECASE | re.DOTALL)
    m = re.search(r"\bselect\b", t, flags=re.IGNORECASE)
    if m:
        t = t[m.start():]
    return t.strip().rstrip(";")

def sql_is_safe(sql: str) -> bool:
    """Permite apenas SELECT e exige presença da tabela alvo; bloqueia DDL/DML e ';'."""
    s = sql.strip()
    s_lower = s.lower()
    if not re.match(r"^\s*select\b", s_lower):
        return False
    forbidden = ["insert", "update", "delete", "merge", "drop", "create", "alter", "truncate", ";", "--", "/*"]
    if any(tok in s_lower for tok in forbidden):
        return False
    target_clean = re.sub(r"[`\s]", "", BQ_TABLE.lower())
    s_clean = re.sub(r"[`\s]", "", s_lower)
    return target_clean in s_clean

def ensure_limit(sql: str, default_limit: int = 1000) -> str:
    s = sql.strip()
    if re.search(r"\blimit\b\s+\d+\s*$", s, flags=re.IGNORECASE):
        return s
    return f"{s}\nLIMIT {default_limit}"

def build_sql_with_ai(question: str, table_fqn: str, columns: list) -> str:
    """Gera SOMENTE SQL BigQuery para a tabela/colunas dadas."""
    if not client:
        return ""
    cols_txt = "\n".join([f"- {c} ({t})" for c, t in columns])
    system = (
        "Você é um gerador de SQL para BigQuery. "
        "Responda SOMENTE com a consulta SQL (sem rótulos, sem explicações, sem cercas de código). "
        "Use exclusivamente a tabela e colunas fornecidas; não use outras tabelas, nem DDL/DML."
    )
    # regras específicas para o schema do export GSC (coluna data_date e sum_top_position)
    user = (
        f"Tabela alvo: `{table_fqn}`.\n"
        f"Colunas disponíveis:\n{cols_txt}\n\n"
        f"Regras específicas:\n"
        f"- Se a pergunta não trouxer período, filtre os últimos 90 dias usando a coluna `data_date`.\n"
        f"- Ao agregar, calcule CTR como SAFE_DIVIDE(SUM(clicks), SUM(impressions)).\n"
        f"- Ao reportar posição média, calcule como SAFE_DIVIDE(SUM(sum_top_position), SUM(impressions)) AS position.\n"
        f"- Quando fizer rankings, ordene por clicks ou impressions e limite resultados longos.\n"
        f"- Comece diretamente com SELECT.\n\n"
        f"Pergunta do usuário:\n{question}\n"
    )
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{"role":"system","content":system},{"role":"user","content":user}],
        temperature=0.1,
    )
    sql = resp.choices[0].message.content.strip()
    return sanitize_sql(sql)

def ai_summary(question: str, df: pd.DataFrame, sql_used: str) -> str:
    """Resumo focado só em fatos; nunca sugira SQL; diga 'Sem dados...' se df vazio."""
    if not client:
        return "Defina OPENAI_API para habilitar a síntese de respostas."
    if df.empty:
        return "Sem dados para o recorte solicitado."

    # compacta uma amostra dos dados como contexto
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

# --------- UI MINIMALISTA ---------
st.markdown("### Chat de Dados – Search Console no BigQuery")

# input no topo (em vez de st.chat_input)
if "messages" not in st.session_state:
    st.session_state.messages = []

with st.form("chat_form", clear_on_submit=True):
    q = st.text_area(
        "Faça sua pergunta:",
        placeholder="Ex.: Top 10 queries mobile no Brasil em agosto de 2024",
        height=80,
    )
    submitted = st.form_submit_button("Enviar")

if submitted and q:
    st.session_state.messages.append({"role":"user","content":q, "sql": None})

# render histórico (em blocos)
schema_cols = []
if bq and BQ_TABLE:
    try:
        schema_cols = get_table_schema(BQ_TABLE)
    except Exception as e:
        st.error(f"Falha ao ler schema da tabela {BQ_TABLE}: {e}")

for i, m in enumerate(st.session_state.messages):
    if m["role"] == "user":
        with st.chat_message("user"):
            st.markdown(m["content"])

        # gerar SQL + executar + responder
        with st.chat_message("assistant"):
            with st.spinner("Analisando dados…"):
                sql = build_sql_with_ai(m["content"], BQ_TABLE, schema_cols)
                if not sql or not sql_is_safe(sql):
                    st.error("Não consegui gerar uma consulta segura para essa pergunta.")
                    if sql:
                        st.code(sql, language="sql")
                    st.session_state.messages[i]["sql"] = sql
                    st.session_state.messages.append({"role":"assistant","content":"Tente refazer a pergunta especificando período e/ou dimensões (ex.: mês, país, device)."})
                else:
                    sql = ensure_limit(sql)
                    try:
                        df = bq.query(sql).result().to_dataframe()
                        answer = ai_summary(m["content"], df, sql)
                        # bloco de resposta
                        st.markdown(answer)

                        # opcionais (menos poluição)
                        if SHOW_TABLE and not df.empty:
                            with st.expander("Ver amostra da tabela"):
                                st.dataframe(df, use_container_width=True)
                        if SHOW_SQL:
                            with st.expander("SQL executada"):
                                st.code(sql, language="sql")

                        st.session_state.messages[i]["sql"] = sql
                        st.session_state.messages.append({"role":"assistant","content":answer})
                    except Exception as e:
                        st.error(f"Erro ao executar a consulta no BigQuery: {e}")

    else:
        # mensagens da IA já renderizadas acima; aqui só mantém histórico
        pass

# auto-scroll para o fim sempre
st.components.v1.html(
    "<script>window.scrollTo(0, document.body.scrollHeight);</script>",
    height=0,
)
