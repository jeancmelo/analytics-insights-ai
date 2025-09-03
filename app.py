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
st.title("Chat de Dados – Search Console no BigQuery")
st.caption("Faça perguntas e receba respostas com números + a SQL usada.")

# --------- ENV VARS ---------
BQ_TABLE = os.getenv("BQ_TABLE", "").strip()  # ex: project.dataset.table
SA_JSON = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

if not BQ_TABLE:
    st.error("Defina a variável de ambiente BQ_TABLE (ex.: projeto.dataset.tabela).")
if not SA_JSON:
    st.error("Defina GOOGLE_APPLICATION_CREDENTIALS_JSON com o conteúdo do JSON da Service Account.")
if not OPENAI_API_KEY:
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

# --------- OPENAI CLIENT (sem herdar proxies do ambiente) ---------
from openai import OpenAI
import httpx

client = None
if OPENAI_API_KEY:
    http_client = httpx.Client(timeout=60.0, follow_redirects=True, trust_env=False)
    client = OpenAI(api_key=OPENAI_API_KEY, http_client=http_client)

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
    # instruções importantes para seu schema:
    # - período padrão: últimos 90 dias usando data_date
    # - CTR: SAFE_DIVIDE(SUM(clicks), SUM(impressions))
    # - posição média: SAFE_DIVIDE(SUM(sum_top_position), SUM(impressions)) AS position
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
        f"- Ao agregar, calcule CTR como SAFE_DIVIDE(SUM(clicks), SUM(impressions)).\n"
        f"- Ao reportar posição média, calcule como SAFE_DIVIDE(SUM(sum_top_position), SUM(impressions)) AS position.\n"
        f"- Quando fizer rankings, ordene por clicks ou impressions e limite resultados longos.\n"
        f"- Não adicione comentários; comece diretamente com SELECT.\n\n"
        f"Pergunta do usuário:\n{question}\n"
    )
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{"role":"system","content":system},{"role":"user","content":user}],
        temperature=0.1,
    )
    sql = resp.choices[0].message.content.strip()
    return sanitize_sql(sql)

def summarize_with_ai(question: str, df: pd.DataFrame, sql_used: str, hints: str = "") -> str:
    if not client:
        return "Defina OPENAI_API para habilitar a síntese de respostas."
    preview = df.head(20).to_csv(index=False)
    system = (
        "Você é um analista de SEO especializado em dados do Google Search Console no BigQuery. "
        "Responda em português claro, com números e comparações quando fizer sentido. "
        "Seja objetivo, cite métricas (clicks, impressions, CTR, position) e apliques restrições da consulta."
    )
    user = (
        f"Pergunta:\n{question}\n\n"
        f"Dicas/filtros aplicados:\n{hints}\n\n"
        f"SQL executada:\n{sql_used}\n\n"
        f"Prévia dos resultados (até 20 linhas em CSV):\n{preview}"
    )
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{"role":"system","content":system},{"role":"user","content":user}],
        temperature=0.2,
    )
    return resp.choices[0].message.content.strip()

# --------- UI ---------
if bq and BQ_TABLE:
    try:
        schema_cols = get_table_schema(BQ_TABLE)
        with st.expander("Esquema da tabela (detecção automática)"):
            st.dataframe(pd.DataFrame(schema_cols, columns=["coluna","tipo"]), use_container_width=True)
    except Exception as e:
        st.error(f"Falha ao ler schema da tabela {BQ_TABLE}: {e}")
        schema_cols = []

    # filtros opcionais (ajudam o modelo)
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        end_default = date.today() - timedelta(days=3)
        start_default = end_default - timedelta(days=90)
        d_ini = st.date_input("Início (opcional)", start_default)
    with c2:
        d_fim = st.date_input("Fim (opcional)", end_default)
    with c3:
        device = st.selectbox("Device (opcional)", ["", "desktop", "mobile", "tablet"], index=0)
    with c4:
        country = st.text_input("País ISO-3 (opcional)", value="")

    site_url = st.text_input("site_url (opcional, prefixo exato)", value="")

    if "messages" not in st.session_state:
        st.session_state.messages = []

    for m in st.session_state.messages:
        with st.chat_message(m["role"]):
            st.markdown(m["content"])

    question = st.chat_input("Ex.: Top 10 queries mobile no Brasil no último mês")
    if question:
        st.session_state.messages.append({"role":"user","content":question})
        with st.chat_message("user"):
            st.markdown(question)

        hints_list = []
        if d_ini and d_fim: hints_list.append(f"Filtrar data_date entre '{d_ini}' e '{d_fim}'")
        if device: hints_list.append(f"device = '{device}'")
        if country: hints_list.append(f"country = '{country.upper()}'")
        if site_url: hints_list.append(f"site_url = '{site_url}'")
        hints = " | ".join(hints_list)

        with st.chat_message("assistant"):
            with st.spinner("Gerando SQL e consultando BigQuery…"):
                # Acrescenta dicas ao prompt para orientar a SQL
                q_plus = question
                if hints:
                    q_plus += f"\n\nConsidere os filtros sugeridos (se fizer sentido): {hints}."

                sql = build_sql_with_ai(q_plus, BQ_TABLE, schema_cols)
                if not sql:
                    st.error("Falha ao gerar SQL. Verifique a variável OPENAI_API.")
                else:
                    # sanity checks
                    if not sql_is_safe(sql):
                        st.error("A SQL gerada não foi considerada segura (somente SELECTs na tabela alvo).")
                        st.code(sql, language="sql")
                    else:
                        sql = ensure_limit(sql)
                        try:
                            df = bq.query(sql).result().to_dataframe()
                            if df.empty:
                                st.info("Sem resultados para os filtros/pergunta.")
                            else:
                                st.dataframe(df, use_container_width=True)
                            answer = summarize_with_ai(question, df, sql, hints)
                            st.markdown(answer)
                            st.session_state.messages.append({"role":"assistant","content":answer})
                            with st.expander("SQL executada"):
                                st.code(sql, language="sql")
                        except Exception as e:
                            st.error(f"Erro ao executar a consulta no BigQuery: {e}")
else:
    st.info("Configure as variáveis de ambiente e permissões do BigQuery para continuar.")
