import os, json, re
import pandas as pd
import streamlit as st
from datetime import date, timedelta

# ========= EMBED NO LOOKER STUDIO =========
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
# =========================================

st.set_page_config(page_title="GSC Chat via BigQuery", layout="wide")

# ======== ENV VARS ========
BQ_TABLE = os.getenv("BQ_TABLE", "").strip()  # ex: project.dataset.table
OPENAI_API_KEY = os.getenv("OPENAI_API", "").strip()
SA_JSON = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON", "").strip()

if not BQ_TABLE:
    st.error("Defina a variável de ambiente BQ_TABLE (ex.: projeto.dataset.tabela).")
if not OPENAI_API_KEY:
    st.warning("Defina OPENAI_API para habilitar o gerador de SQL/respostas.")
if not SA_JSON:
    st.error("Defina GOOGLE_APPLICATION_CREDENTIALS_JSON com o conteúdo do JSON da Service Account.")

# escrever credencial em arquivo temporário (padrão GCP SDK)
if SA_JSON:
    SA_PATH = "/tmp/sa.json"
    with open(SA_PATH, "w") as f:
        f.write(SA_JSON)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_PATH

# ======== BIGQUERY CLIENT ========
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

# ======== OPENAI CLIENT ========
from openai import OpenAI
client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ======== UTIL ========
@st.cache_data(show_spinner=False)
def get_table_schema(table_fqn: str):
    """Retorna colunas e tipos da tabela alvo."""
    tbl = bq.get_table(table_fqn)
    cols = [(s.name, s.field_type) for s in tbl.schema]
    return cols

def sql_is_safe(sql: str) -> bool:
    """Permite somente SELECTs no dataset/table especificado; bloqueia DML/DDL e cross-dataset."""
    s = sql.strip().lower()
    if not s.startswith("select"):
        return False
    forbidden = ["insert", "update", "delete", "merge", "drop", "create", "alter", "truncate", ";", "--", "/*"]
    if any(tok in s for tok in forbidden):
        return False
    # impede consultas fora da tabela alvo (não perfeito, mas bom para MVP)
    # aceita alias/CTE desde que a FROM principal contenha a tabela alvo
    target = BQ_TABLE.lower()
    if target not in s:
        return False
    return True

def ensure_limit(sql: str, default_limit: int = 2000) -> str:
    """Garante um LIMIT no final para evitar respostas gigantescas."""
    s = sql.strip()
    if re.search(r"\blimit\b\s+\d+\s*;?\s*$", s, flags=re.IGNORECASE):
        return s
    return f"{s}\nLIMIT {default_limit}"

def summarize_with_ai(question: str, df: pd.DataFrame, sql_used: str, extra_context: str = "") -> str:
    if not client:
        return "Defina OPENAI_API para habilitar a síntese de respostas."
    # cria um resumo compacto do dataframe para dar contexto
    preview = df.head(20).to_csv(index=False)
    system = (
        "Você é um analista de SEO especializado em dados do Google Search Console no BigQuery. "
        "Responda em português brasileiro, de forma direta e com números. "
        "Seja transparente sobre limites/amostragem. Não invente dados que não estejam na tabela."
    )
    user = (
        f"Pergunta do usuário:\n{question}\n\n"
        f"Contexto adicional (schema/observações):\n{extra_context}\n\n"
        f"SQL executada:\n{sql_used}\n\n"
        f"Prévia dos resultados (até 20 linhas em CSV):\n{preview}"
    )
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role":"system","content":system},{"role":"user","content":user}],
        temperature=0.2,
    )
    return resp.choices[0].message.content.strip()

def build_sql_with_ai(question: str, table_fqn: str, columns: list, date_col_guess="date") -> str:
    """Pede ao modelo que gere SOMENTE SQL, restrita à tabela e colunas indicadas."""
    if not client:
        return ""
    cols_txt = "\n".join([f"- {c} ({t})" for c,t in columns])
    system = (
        "Você é um gerador de SQL BigQuery. Produza apenas a consulta SQL (sem comentários), "
        "começando com SELECT e usando exclusivamente a tabela e colunas fornecidas. "
        "Nunca use DDL/DML, nunca use subconsultas externas ou outras tabelas. "
        "Prefira agregações claras e inclua ORDER BY quando adequado."
    )
    user = (
        f"Tabela alvo: `{table_fqn}`.\n"
        f"Colunas disponíveis:\n{cols_txt}\n\n"
        f"Regras:\n"
        f"- Use somente `{table_fqn}`.\n"
        f"- Se a pergunta não especificar período, use os últimos 90 dias com base na coluna `{date_col_guess}` se existir.\n"
        f"- Se fizer SUM de clicks/impressions, calcule CTR como SAFE_DIVIDE(SUM(clicks), SUM(impressions)) e posição média como AVG(position).\n"
        f"- Limite resultados mais longos com LIMIT 1000.\n\n"
        f"Pergunta do usuário:\n{question}\n\n"
        f"Responda apenas com a SQL."
    )
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role":"system","content":system},{"role":"user","content":user}],
        temperature=0.1,
    )
    sql = resp.choices[0].message.content.strip().strip("`")
    # remove cercas de código se vierem
    sql = re.sub(r"^```sql\s*|\s*```$", "", sql, flags=re.IGNORECASE|re.DOTALL).strip()
    return sql

# ======== UI ========
st.title("Chat de Dados – GSC no BigQuery")
st.caption("Pergunte sobre seus dados do Search Console exportados para o BigQuery.")

if bq and BQ_TABLE:
    try:
        schema_cols = get_table_schema(BQ_TABLE)
        with st.expander("Esquema da tabela (detecção automática)"):
            st.write(pd.DataFrame(schema_cols, columns=["coluna","tipo"]))
    except Exception as e:
        st.error(f"Falha ao ler o schema da tabela {BQ_TABLE}: {e}")
        schema_cols = []

    # filtros auxiliares (opcionais, ajudam o modelo)
    col1, col2, col3 = st.columns(3)
    with col1:
        has_date = any(c[0].lower() == "date" for c in schema_cols)
        if has_date:
            end_default = date.today() - timedelta(days=3)
            start_default = end_default - timedelta(days=90)
            d_ini = st.date_input("Início (opcional)", start_default)
            d_fim = st.date_input("Fim (opcional)", end_default)
        else:
            d_ini = d_fim = None
    with col2:
        device = st.selectbox("Device (opcional)", ["", "desktop", "mobile", "tablet"], index=0)
    with col3:
        country = st.text_input("País ISO-3 (opcional)", value="")

    # chat
    if "messages" not in st.session_state:
        st.session_state.messages = []

    for m in st.session_state.messages:
        with st.chat_message(m["role"]):
            st.markdown(m["content"])

    question = st.chat_input("Faça sua pergunta (ex.: Top 10 queries mobile no Brasil no último mês)")
    if question:
        st.session_state.messages.append({"role":"user","content":question})
        with st.chat_message("user"):
            st.markdown(question)

        # contexto extra para orientar o SQL
        hints = []
        if d_ini and d_fim: hints.append(f"Período sugerido: {d_ini} a {d_fim}. (coluna date)")
        if device: hints.append(f"Filtrar device = '{device}'.")
        if country: hints.append(f"Filtrar country = '{country.upper()}'.")
        extra_ctx = " | ".join(hints)

        with st.chat_message("assistant"):
            with st.spinner("Gerando SQL e consultando BigQuery…"):
                sql = build_sql_with_ai(f"{question}\n\n{extra_ctx}", BQ_TABLE, schema_cols)
                if not sql:
                    st.error("Falha ao gerar SQL. Verifique a OPENAI_API.")
                else:
                    # sanity checks
                    if not sql_is_safe(sql):
                        st.error("A SQL gerada não foi considerada segura (apenas SELECTs na tabela alvo são permitidos).")
                        st.code(sql, language="sql")
                    else:
                        sql = ensure_limit(sql)
                        try:
                            df = bq.query(sql).result().to_dataframe()
                            if df.empty:
                                st.info("Sem resultados para os filtros/pergunta.")
                            else:
                                st.dataframe(df, use_container_width=True)
                            # resposta em NL
                            answer = summarize_with_ai(question, df, sql, extra_context=extra_ctx)
                            st.markdown(answer)
                            st.session_state.messages.append({"role":"assistant","content":answer})
                            with st.expander("SQL executada"):
                                st.code(sql, language="sql")
                        except Exception as e:
                            st.error(f"Erro ao executar a consulta no BigQuery: {e}")
else:
    st.info("Configure as variáveis de ambiente e permissões do BigQuery para continuar.")
