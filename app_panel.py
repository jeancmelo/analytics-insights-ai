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
# (compatibilidade) caso uses apenas 1 tabela
BQ_TABLE = os.getenv("BQ_TABLE", "").strip()

# Views recomendadas (Rubis Gas) — podes sobrescrever por ENV no Railway
DEFAULT_FACT_GA4 = "wyp-analytics.wyp_gold_client_rubis_gas.vw_fact_ga4_page_day"
DEFAULT_FACT_GSC = "wyp-analytics.wyp_gold_client_rubis_gas.vw_fact_gsc_page_day"
DEFAULT_AI_READY = "wyp-analytics.wyp_gold_client_rubis_gas.vw_ai_page_performance_day"

BQ_VIEW_FACT_GA4 = os.getenv("BQ_VIEW_FACT_GA4", DEFAULT_FACT_GA4).strip()
BQ_VIEW_FACT_GSC = os.getenv("BQ_VIEW_FACT_GSC", DEFAULT_FACT_GSC).strip()
BQ_VIEW_AI_READY = os.getenv("BQ_VIEW_AI_READY", DEFAULT_AI_READY).strip()

SA_JSON      = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON", "").strip()
OPENAI_KEY   = os.getenv("OPENAI_API", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

if not SA_JSON:
    st.error("Defina GOOGLE_APPLICATION_CREDENTIALS_JSON (conteúdo do JSON da Service Account).")
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
from google.api_core.exceptions import Forbidden, NotFound, BadRequest

@st.cache_resource(show_spinner=False)
def get_bq(): 
    return bigquery.Client()

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

# --------- STYLE ---------
st.markdown("""
<style>
[data-testid="stAppViewContainer"] .chips .stButton > button {
  background: #f8fafc !important;
  background-color: #f8fafc !important;
  color: #111827 !important;
  border: 1px solid #e5e7eb !important;
  box-shadow: none !important;
}
[data-testid="stAppViewContainer"] .chips .stButton > button:hover {
  background: #f1f5f9 !important;
  background-color: #f1f5f9 !important;
  border-color: #cbd5e1 !important;
}
.chips .stButton > button p,
.chips .stButton > button span { color:#111827 !important; }

[data-testid="stCaption"] { color:#374151 !important; }

.btn-row { display:grid !important; grid-template-columns: 1fr !important; gap:8px !important; }
.btn-row .stButton { margin:0 !important; }

[class^="st-emotion-cache-"] { gap: 0 !important; row-gap: 5px !important;}
li[class^="st-emotion-cache-"],
li[class*=" st-emotion-cache-"] { margin-bottom: 6% !important; padding: 0px 0px 0px 0.6em !important;}

.block-container  {padding: 3rem 1rem 10rem  !important;}
.kf-list { counter-reset:item; list-style:none; padding-left:0; margin:0; }
.kf-list li { counter-increment:item; margin:.55rem 0; }
.kf-list li::before { content: counter(item) "."; font-weight:700; margin-right:.35rem; color:#111827; }
.kf-item-title { font-weight:700; }
.kf-item-text { display:block; margin-top:.15rem; }
.kf-title { font-weight:700; margin-bottom: 5%;}

.divider{ height:1px; background:#e5e7eb; margin:.6rem 0; }
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

def sql_is_safe(sql: str, allowed_table_fqn: str) -> bool:
    s = sql.strip().lower()
    if not re.match(r"^\s*select\b", s): return False
    forbidden = ["insert","update","delete","merge","drop","create","alter","truncate",";","--","/*"]
    if any(tok in s for tok in forbidden): return False
    target_clean = re.sub(r"[`\s]","", allowed_table_fqn.lower())
    s_clean      = re.sub(r"[`\s]","", s)
    return target_clean in s_clean

def ensure_limit(sql: str, default_limit:int=1000) -> str:
    return sql if re.search(r"\blimit\b\s+\d+\s*$", sql, re.I) else f"{sql}\nLIMIT {default_limit}"

# --------- Router (Auto) ---------
def _route_dataset_heuristic(question: str) -> str:
    q = (question or "").lower()
    has_gsc = any(k in q for k in ["impression","impress","click","ctr","position","search console","gsc","seo"])
    has_ga4 = any(k in q for k in ["session","sessions","pageview","pageviews","ga4","engagement","bounce","event"])
    has_why = any(k in q for k in ["why","porque","por que","causa","driver","explica","explain","justify","justificar","motivo","razão","razao"])
    if has_why and (has_gsc or has_ga4): return "AI_READY"
    if has_gsc and has_ga4: return "AI_READY"
    if has_gsc and not has_ga4: return "FACT_GSC"
    if has_ga4 and not has_gsc: return "FACT_GA4"
    return "AI_READY"

def route_view(question: str) -> str:
    """Escolhe qual camada usar. Primeiro tenta LLM (router). Se falhar, usa heurística."""
    if not client:
        return _route_dataset_heuristic(question)

    system = (
        "You are a routing assistant for an analytics tool. "
        "Choose which dataset/view to use to answer the user's question. "
        "Return ONLY JSON with key 'dataset'."
    )
    user = (
        "Available options:\n"
        "- AI_READY: explanations/causes or correlations between SEO (GSC) and onsite behavior (GA4).\n"
        "- FACT_GSC: SEO-only about impressions, clicks, CTR, rankings, position.\n"
        "- FACT_GA4: onsite-only about sessions, pageviews, engagement.\n\n"
        f"User question: {question}\n\n"
        "Return JSON like: {\"dataset\":\"AI_READY\"}"
    )
    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role":"system","content":system},{"role":"user","content":user}],
            temperature=0.0,
            response_format={"type":"json_object"}
        )
        data = json.loads(resp.choices[0].message.content or "{}")
        ds = str(data.get("dataset","")).strip().upper()
        if ds in {"AI_READY","FACT_GSC","FACT_GA4"}:
            return ds
        return _route_dataset_heuristic(question)
    except Exception:
        return _route_dataset_heuristic(question)

def _resolve_table_from_dataset(dataset_code: str):
    if dataset_code == "FACT_GSC":
        return BQ_VIEW_FACT_GSC, "FACT_GSC", "Rubis Gas – FACT (GSC por URL/dia)"
    if dataset_code == "FACT_GA4":
        return BQ_VIEW_FACT_GA4, "FACT_GA4", "Rubis Gas – FACT (GA4 por URL/dia)"
    return BQ_VIEW_AI_READY, "AI_READY", "Rubis Gas – AI Ready (GA4 + GSC)"

# --------- LLM prompts (gera SQL e depois findings em JSON) ---------
def build_sql_with_ai(question: str, table_fqn: str, columns: list, table_kind: str) -> str:
    if not client: return ""
    cols_txt = "\n".join([f"- {c} ({t})" for c, t in columns])

    system = (
        "Você é um gerador de SQL para BigQuery. "
        "Responda SOMENTE com a consulta SQL (sem rótulos, sem explicações, sem cercas de código). "
        "Use exclusivamente a tabela e colunas fornecidas; não use outras tabelas, nem DDL/DML."
    )

    if table_kind == "AI_READY":
        rules = (
            "- Se a pergunta não trouxer período, filtre os últimos 90 dias usando a coluna `data_date`.\n"
            "- Métricas GSC: clicks=SUM(gsc_clicks), impressions=SUM(gsc_impressions), ctr=SAFE_DIVIDE(SUM(gsc_clicks), SUM(gsc_impressions)), avg_position=AVG(gsc_avg_position).\n"
            "- Métricas GA4: pageviews=SUM(ga_pageviews), sessions=SUM(ga_sessions).\n"
            "- Para análise temporal, pode usar deltas (delta_*_7d). Para diagnóstico, pode usar flags (flag_*).\n"
            "- Para rankings, ordene por impressions, clicks, sessions ou pageviews e limite resultados longos.\n"
        )
    elif table_kind == "FACT_GSC":
        rules = (
            "- Se a pergunta não trouxer período, filtre os últimos 90 dias usando a coluna `data_date`.\n"
            "- Métricas: clicks=SUM(gsc_clicks), impressions=SUM(gsc_impressions), ctr=SAFE_DIVIDE(SUM(gsc_clicks), SUM(gsc_impressions)), avg_position=AVG(gsc_avg_position).\n"
            "- Para rankings, ordene por impressions ou clicks e limite resultados longos.\n"
        )
    else:  # FACT_GA4
        rules = (
            "- Se a pergunta não trouxer período, filtre os últimos 90 dias usando a coluna `data_date`.\n"
            "- Métricas: pageviews=SUM(ga_pageviews), sessions=SUM(ga_sessions).\n"
            "- Para rankings, ordene por sessions ou pageviews e limite resultados longos.\n"
        )

    user = (
        f"Tabela alvo: `{table_fqn}`.\n"
        f"Colunas disponíveis:\n{cols_txt}\n\n"
        f"Regras específicas:\n{rules}"
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
        out = []
        for it in findings[:n]:
            title = str(it.get("title","Insight")).strip()[:120]
            text  = str(it.get("text","")).strip()
            if text:
                out.append({"title":title or "Insight", "text":text})
        return out or [{"title":"Sem insights","text":"Os dados retornados são muito curtos para gerar achados úteis."}]
    except Exception:
        return [{"title":"Resumo","text": resp.choices[0].message.content.strip()}]

# --------- STATE ---------
if "insights" not in st.session_state:
    st.session_state.insights = []
if "pending" not in st.session_state:
    st.session_state.pending = None

# --------- UI ---------
st.markdown("### Generative Insights")
with st.container():
    st.markdown('<div class="panel-card">', unsafe_allow_html=True)

    source = st.selectbox(
        "Data source",
        [
            "Auto (Router) – Rubis Gas (recomendado)",
            "Rubis Gas – AI Ready (GA4 + GSC)",
            "Rubis Gas – FACT (GSC por URL/dia)",
            "Rubis Gas – FACT (GA4 por URL/dia)",
            "Instagram Insights (Supermetrics)",
            "Facebook Page Insights (Supermetrics)",
        ],
        index=0
    )

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    st.caption("Quick prompts")
    c1, c2 = st.columns(2)
    with c1: chip1 = st.button("Key findings for this period", key="chip1")
    with c2: chip2 = st.button("Compare with last month", key="chip2")
    c3, c4 = st.columns(2)
    with c3: chip3 = st.button("Top pages (and drivers)", key="chip3")
    with c4: chip4 = st.button("Any anomalies to highlight?", key="chip4")

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

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

    st.markdown('</div>', unsafe_allow_html=True)

# Chips
if chip1: q, send = "Give me 5 key findings for the current period.", True
if chip2: q, send = "Summarize performance vs last month in up to 5 findings.", True
if chip3: q, send = "Show the top pages and explain what is driving their performance.", True
if chip4: q, send = "Detect anomalies or significant day-to-day changes worth attention.", True

if clear:
    st.session_state.insights = []
    st.session_state.pending = None
    st.rerun()

if send and q and q.strip():
    st.session_state.insights.insert(0, {"q": q.strip(), "findings": None, "ts": time.time(), "sql": None, "route": None})
    st.session_state.pending = 0
    st.rerun()

def _active_bq_table_and_kind(selected_source: str):
    if selected_source.startswith("Rubis Gas – AI Ready"):
        return BQ_VIEW_AI_READY, "AI_READY", "Rubis Gas – AI Ready (GA4 + GSC)"
    if selected_source.startswith("Rubis Gas – FACT (GSC"):
        return BQ_VIEW_FACT_GSC, "FACT_GSC", "Rubis Gas – FACT (GSC por URL/dia)"
    if selected_source.startswith("Rubis Gas – FACT (GA4"):
        return BQ_VIEW_FACT_GA4, "FACT_GA4", "Rubis Gas – FACT (GA4 por URL/dia)"
    if BQ_TABLE:
        return BQ_TABLE, "AI_READY", f"Manual – {BQ_TABLE}"
    return "", "AI_READY", "N/A"

# Processa UMA pendência
if st.session_state.pending is not None:
    idx = st.session_state.pending
    try:
        q_user = st.session_state.insights[idx]["q"]
        current_source = source

        # ---------------- BigQuery (Rubis Gas) ----------------
        if current_source.startswith("Auto (Router)") or current_source.startswith("Rubis Gas"):
            if not bq:
                raise RuntimeError("BigQuery não inicializado. Verifique GOOGLE_APPLICATION_CREDENTIALS_JSON.")

            if current_source.startswith("Auto (Router)"):
                ds = route_view(q_user)
                active_table, table_kind, route_label = _resolve_table_from_dataset(ds)
            else:
                active_table, table_kind, route_label = _active_bq_table_and_kind(current_source)

            if not active_table:
                raise RuntimeError("Nenhuma tabela/view configurada. Defina BQ_VIEW_* ou BQ_TABLE.")

            try:
                schema_cols = get_table_schema(active_table)
            except Forbidden as e:
                raise RuntimeError(
                    "A Service Account não tem permissão para ler o schema da VIEW. "
                    "Garanta no mínimo: bigquery.tables.get (e para rodar query: bigquery.jobs.create / bigquery.dataViewer). "
                    f"Detalhe: {e}"
                )
            except NotFound:
                raise RuntimeError(f"A VIEW não existe (ou está noutro dataset/projeto). Confirme o nome: {active_table}")

            sql = build_sql_with_ai(q_user, active_table, schema_cols, table_kind)

            if not sql or not sql_is_safe(sql, active_table):
                st.session_state.insights[idx]["findings"] = [
                    {"title":"Consulta inválida","text":"Não foi possível gerar uma SQL segura. Refine a pergunta."}
                ]
                st.session_state.insights[idx]["sql"] = sql or ""
                st.session_state.insights[idx]["route"] = route_label
            else:
                sql = ensure_limit(sql)
                try:
                    df  = bq.query(sql).result().to_dataframe()
                except Forbidden as e:
                    raise RuntimeError(
                        "Permissão insuficiente para executar queries. Garanta: bigquery.jobs.create no projeto e acesso de leitura "
                        "nas tabelas base usadas pela VIEW. "
                        f"Detalhe: {e}"
                    )
                except BadRequest as e:
                    raise RuntimeError(f"SQL inválida gerada. Detalhe: {e}")

                findings = ai_key_findings(q_user, df, sql, n=6)
                st.session_state.insights[idx]["findings"] = findings
                st.session_state.insights[idx]["sql"] = sql
                st.session_state.insights[idx]["route"] = route_label

        elif current_source.startswith("Instagram"):
            ig = instagram_adapter_from_env()
            fields_env = os.getenv("IGI_FIELDS", "month,followers_count,follows_count").split(",")
            has_month = any(f.strip().lower() == "month" for f in fields_env)
            gran = None if has_month else "day"

            drt = (os.getenv("IGI_DATE_RANGE_TYPE") or "").strip() or None
            if drt:
                df = ig.query(fields=[f.strip() for f in fields_env], date_range_type=drt, time_granularity=gran)
                sql_ctx = f"Supermetrics IGI fields={','.join([f.strip() for f in fields_env])} range={drt}"
            else:
                end = date.today()
                start = end - timedelta(days=30)
                df = ig.query(fields=[f.strip() for f in fields_env], date_from=start.isoformat(), date_to=end.isoformat(), time_granularity=gran)
                sql_ctx = f"Supermetrics IGI fields={','.join([f.strip() for f in fields_env])} {start}..{end}"

            findings = ai_key_findings(q_user, df, sql_ctx, n=6)
            st.session_state.insights[idx]["findings"] = findings
            st.session_state.insights[idx]["sql"] = "Supermetrics (IGI)"
            st.session_state.insights[idx]["route"] = "Instagram (Supermetrics)"

        elif current_source.startswith("Facebook"):
            fb = facebook_pages_adapter_from_env()
            end = date.today()
            start = end - timedelta(days=30)
            fields = os.getenv(
                "FPI_FIELDS",
                "date,page_id,post_id,permalink,post_type,message,post_reach,post_impressions,post_engaged_users,reactions_total,comments,shares,link_clicks,video_views"
            ).split(",")

            df = fb.query(fields=[f.strip() for f in fields], date_from=start.isoformat(), date_to=end.isoformat(), time_granularity="day")
            findings = ai_key_findings(q_user, df, f"Supermetrics FB fields={','.join(fields)} {start}..{end}", n=6)
            st.session_state.insights[idx]["findings"] = findings
            st.session_state.insights[idx]["sql"] = "Supermetrics (FB)"
            st.session_state.insights[idx]["route"] = "Facebook (Supermetrics)"

        else:
            st.session_state.insights[idx]["findings"] = [{"title":"Fonte não suportada","text":"Selecione uma fonte válida."}]
            st.session_state.insights[idx]["sql"] = ""
            st.session_state.insights[idx]["route"] = "N/A"

    except Exception as e:
        st.session_state.insights[idx]["findings"] = [{"title":"Erro ao consultar","text": str(e)}]
        st.session_state.insights[idx]["sql"] = ""
        st.session_state.insights[idx]["route"] = None
    finally:
        st.session_state.pending = None
        st.rerun()

# --------- Render: Key Findings (mais recente) ---------
if st.session_state.insights:
    block = st.session_state.insights[0]
    st.markdown('<div class="card kf-card">', unsafe_allow_html=True)
    st.markdown('<div class="kf-title">Key Findings</div>', unsafe_allow_html=True)

    route_used = block.get("route")
    if route_used:
        st.markdown(
            f"<div style='color:#6b7280;font-size:12px;margin-bottom:8px;'>View usada: {escape(str(route_used))}</div>",
            unsafe_allow_html=True
        )

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

    with st.expander("SQL usada (debug)"):
        st.code(block.get("sql") or "", language="sql")
else:
    st.info("Use os quick prompts acima ou escreva sua pergunta e clique em **Send** para gerar os insights.")