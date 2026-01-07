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

# ---------------- ENV VARS ----------------
BQ_TABLE = os.getenv("BQ_TABLE", "").strip()  # fallback

DEFAULT_FACT_GA4 = "wyp-analytics.wyp_gold_client_rubis_gas.vw_fact_ga4_page_day"
DEFAULT_FACT_GSC = "wyp-analytics.wyp_gold_client_rubis_gas.vw_fact_gsc_page_day"
DEFAULT_AI_READY = "wyp-analytics.wyp_gold_client_rubis_gas.vw_ai_page_performance_day"

BQ_VIEW_FACT_GA4 = os.getenv("BQ_VIEW_FACT_GA4", DEFAULT_FACT_GA4).strip()
BQ_VIEW_FACT_GSC = os.getenv("BQ_VIEW_FACT_GSC", DEFAULT_FACT_GSC).strip()
BQ_VIEW_AI_READY = os.getenv("BQ_VIEW_AI_READY", DEFAULT_AI_READY).strip()

SA_JSON = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON", "").strip()
OPENAI_KEY = os.getenv("OPENAI_API", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip()

# Logo (black SVG) provided by you
WYP_LOGO_URL = "https://wyperformance.com/wp-content/themes/wyp/dist/img/logo-wyperformance-black.svg"

# Subtle brand accents (use sparingly)
BRAND_ACCENT = "#F15A24"

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

# ---------------- BigQuery ----------------
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

# ---------------- OpenAI (sem herdar proxies) ----------------
from openai import OpenAI
import httpx

client = None
if OPENAI_KEY:
    http_client = httpx.Client(timeout=60.0, follow_redirects=True, trust_env=False)
    client = OpenAI(api_key=OPENAI_KEY, http_client=http_client)

# ---------------- STYLE ----------------
st.markdown(
    f"""
<style>
/* Base */
[data-testid="stAppViewContainer"] {{
  background: #0B0F14;
}}
.block-container {{
  padding: 20px 18px 160px !important; /* espaço para o composer */
  max-width: 520px !important;
}}
/* Ajuste de “top bar” (Railway / browser overlay) */
.ai-header {{
  position: sticky;
  top: 44px; /* desce mais para não colidir com barras do host (Railway) */
  z-index: 50;
  padding: 22px 14px 12px; /* mais margem no topo para a logo */
  background: linear-gradient(180deg, rgba(11,15,20,0.98) 0%, rgba(11,15,20,0.90) 60%, rgba(11,15,20,0.0) 100%);
  backdrop-filter: blur(10px);
}}
.ai-topbar {{
  display: grid;
  grid-template-columns: 1fr auto 1fr;
  align-items: center;
  gap: 10px;
}}
.ai-left-spacer {{
  color: rgba(255,255,255,0.55);
  font-size: 12px;
}}
.logo-pill {{
  justify-self: center;
  background: rgba(255,255,255,0.95);
  border: 1px solid rgba(0,0,0,0.08);
  border-radius: 14px;
  padding: 8px 12px;
  display: flex;
  align-items: center;
  box-shadow: 0 10px 30px rgba(0,0,0,0.35);
}}
.logo-pill img {{
  height: 18px;
  display:block;
}}
.ai-right-spacer {{
  justify-self: end;
  font-size: 12px;
  color: rgba(255,255,255,0.30);
}}
.accent-line {{
  height: 2px;
  background: linear-gradient(90deg, {BRAND_ACCENT} 0%, rgba(241,90,36,0.05) 70%, rgba(241,90,36,0.0) 100%);
  border-radius: 999px;
  margin-top: 10px;
}}

/* “Bubble” visual consistente (para summary e respostas) */
.bubble {{
  background: rgba(255,255,255,0.04);
  border: 1px solid rgba(255,255,255,0.08);
  border-radius: 18px;
  padding: 14px 14px;
  box-shadow: 0 14px 40px rgba(0,0,0,0.35);
}}
.bubble-title {{
  font-size: 14px;
  font-weight: 800;
  color: rgba(255,255,255,0.92);
  margin: 0 0 10px 0;
}}
.bubble-muted {{
  color: rgba(255,255,255,0.70);
  font-size: 12.8px;
  line-height: 1.4;
}}

.finding-card {{
  background: rgba(255,255,255,0.03);
  border: 1px solid rgba(255,255,255,0.08);
  border-radius: 14px;
  padding: 10px 12px;
  margin: 10px 0;
}}
.finding-title {{
  font-weight: 800;
  color: rgba(255,255,255,0.92);
  font-size: 12.8px;
  margin-bottom: 4px;
}}
.finding-text {{
  color: rgba(255,255,255,0.72);
  font-size: 12.7px;
  line-height: 1.35;
}}

/* Datasource badge */
.ds-row {{
  display:flex;
  align-items:center;
  justify-content: space-between;
  gap: 10px;
  margin-top: 8px;
}}
.ds-badge {{
  font-size: 12px;
  color: rgba(255,255,255,0.65);
  border: 1px solid rgba(255,255,255,0.10);
  padding: 6px 10px;
  border-radius: 999px;
  background: rgba(255,255,255,0.03);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}}

/* Composer fixo */
.composer {{
  position: fixed;
  right: 18px;
  bottom: 16px;
  width: 480px;
  max-width: calc(100vw - 36px);
  z-index: 60;
}}
.composer-inner {{
  background: rgba(11,15,20,0.92);
  border: 1px solid rgba(255,255,255,0.10);
  border-radius: 18px;
  padding: 10px;
  backdrop-filter: blur(12px);
  box-shadow: 0 18px 60px rgba(0,0,0,0.55);
}}

/* Buttons */
.stButton > button {{
  border-radius: 12px !important;
  border: 1px solid rgba(255,255,255,0.10) !important;
  background: rgba(255,255,255,0.05) !important;
  color: rgba(255,255,255,0.92) !important;
}}
.stButton > button:hover {{
  border-color: rgba(255,255,255,0.18) !important;
  background: rgba(255,255,255,0.07) !important;
}}
.send-btn .stButton > button {{
  background: {BRAND_ACCENT}15 !important;
  border-color: {BRAND_ACCENT}55 !important;
}}
.send-btn .stButton > button:hover {{
  background: {BRAND_ACCENT}22 !important;
  border-color: {BRAND_ACCENT}77 !important;
}}

/* Selectbox blend */
[data-testid="stSelectbox"] > div {{
  background: rgba(255,255,255,0.04) !important;
  border: 1px solid rgba(255,255,255,0.10) !important;
  border-radius: 14px !important;
}}
label, .stCaption {{
  color: rgba(255,255,255,0.65) !important;
}}
</style>
""",
    unsafe_allow_html=True,
)

# ---------------- Helpers: SQL ----------------
def sanitize_sql(text: str) -> str:
    if not text:
        return ""
    t = text.strip()
    t = re.sub(r"^sql\s*", "", t, flags=re.IGNORECASE)
    t = re.sub(r"^```(?:sql)?\s*|\s*```$", "", t, flags=re.IGNORECASE | re.DOTALL)
    m = re.search(r"\bselect\b", t, flags=re.IGNORECASE)
    if m:
        t = t[m.start():]
    return t.strip().rstrip(";")

def sql_is_safe(sql: str, allowed_table_fqn: str) -> bool:
    s = sql.strip().lower()
    if not re.match(r"^\s*select\b", s):
        return False
    forbidden = ["insert", "update", "delete", "merge", "drop", "create", "alter", "truncate", ";", "--", "/*"]
    if any(tok in s for tok in forbidden):
        return False

    target_clean = re.sub(r"[`\s]", "", allowed_table_fqn.lower())
    s_clean = re.sub(r"[`\s]", "", s)
    return target_clean in s_clean

def ensure_limit(sql: str, default_limit: int = 1000) -> str:
    return sql if re.search(r"\blimit\b\s+\d+\s*$", sql, re.I) else f"{sql}\nLIMIT {default_limit}"

def build_sql_with_ai(question: str, table_fqn: str, columns: list, table_kind: str) -> str:
    if not client:
        return ""

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
    else:
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
        messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
        temperature=0.1,
    )
    return sanitize_sql(resp.choices[0].message.content.strip())

def ai_key_findings(question: str, df: pd.DataFrame, sql_used: str, n: int = 6):
    if not client:
        return [{"title": "Configuração necessária", "text": "Defina OPENAI_API."}]
    if df.empty:
        return [{"title": "Sem dados", "text": "Não há linhas para o recorte solicitado."}]

    preview = df.head(40).to_csv(index=False)

    system = (
        "Você é um analista de Marketing/SEO. Gere insights curtos e acionáveis "
        "com base nos dados fornecidos. Responda em JSON válido com a chave 'findings'. "
        "Não invente números; use apenas o que vier nos dados."
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
        messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
        temperature=0.2,
        response_format={"type": "json_object"},
    )

    try:
        data = json.loads(resp.choices[0].message.content or "{}")
        findings = data.get("findings", [])
        out = []
        for it in findings[:n]:
            title = str(it.get("title", "Insight")).strip()[:120]
            text = str(it.get("text", "")).strip()
            if text:
                out.append({"title": title or "Insight", "text": text})
        return out or [{"title": "Sem insights", "text": "Os dados retornados são muito curtos para gerar achados úteis."}]
    except Exception:
        return [{"title": "Resumo", "text": resp.choices[0].message.content.strip()}]

def _active_bq_table_and_kind(selected_source: str):
    if selected_source.startswith("Rubis Gas – AI Ready"):
        return BQ_VIEW_AI_READY, "AI_READY"
    if selected_source.startswith("Rubis Gas – FACT (GSC"):
        return BQ_VIEW_FACT_GSC, "FACT_GSC"
    if selected_source.startswith("Rubis Gas – FACT (GA4"):
        return BQ_VIEW_FACT_GA4, "FACT_GA4"
    if BQ_TABLE:
        return BQ_TABLE, "AI_READY"
    return "", "AI_READY"

# ---------------- STATE ----------------
if "messages" not in st.session_state:
    st.session_state.messages = []
if "active_source" not in st.session_state:
    st.session_state.active_source = None
if "summary_cached" not in st.session_state:
    st.session_state.summary_cached = {}
if "pending_job" not in st.session_state:
    st.session_state.pending_job = None

# ---------------- HEADER ----------------
st.markdown(
    f"""
<div class="ai-header">
  <div class="ai-topbar">
    <div class="ai-left-spacer"></div>
    <div class="logo-pill">
      <img src="{WYP_LOGO_URL}" alt="Wyperformance" />
    </div>
    <div class="ai-right-spacer"></div>
  </div>
  <div class="accent-line"></div>
</div>
""",
    unsafe_allow_html=True,
)

# ---------------- DATASOURCE SELECT ----------------
source = st.selectbox(
    "Fonte de dados",
    [
        "Rubis Gas – AI Ready (GA4 + GSC + Screaming Frog)",
        "Rubis Gas – FACT (GSC por URL/dia)",
        "Rubis Gas – FACT (GA4 por URL/dia)",
        "Instagram Insights (Supermetrics)",
        "Facebook Page Insights (Supermetrics)",
    ],
    index=0,
)

st.markdown(
    f"""
<div class="ds-row">
  <div class="ds-badge">Usando: {escape(source)}</div>
</div>
""",
    unsafe_allow_html=True,
)

def _push_assistant_summary_for_source(selected_source: str):
    if selected_source in st.session_state.summary_cached:
        return

    if selected_source.startswith("Rubis Gas"):
        q = (
            "Crie um resumo geral bem curto do site para o período selecionado em 3-5 bullets. "
            "Foque no que mais importa (SEO técnico, indexação, CTR e performance)."
        )
    else:
        q = (
            "Crie um resumo geral de performance para o período selecionado em 3-5 bullets. "
            "Foque em mudanças e próximos passos."
        )

    st.session_state.pending_job = {"kind": "summary", "source": selected_source, "question": q}

def _enqueue_user_question(selected_source: str, question: str):
    st.session_state.messages.append({"role": "user", "type": "text", "text": question.strip(), "ts": time.time()})
    st.session_state.pending_job = {"kind": "chat", "source": selected_source, "question": question.strip()}

# Troca de fonte -> injeta summary (uma vez)
if st.session_state.active_source != source:
    st.session_state.active_source = source
    _push_assistant_summary_for_source(source)
    st.rerun()

# ---------------- RENDER CHAT ----------------
for m in st.session_state.messages:
    if m["role"] == "user":
        with st.chat_message("user"):
            st.markdown(escape(m.get("text", "")))
    else:
        with st.chat_message("assistant"):
            if m.get("type") == "summary":
                # Importante: renderizar o bubble em UMA única chamada para evitar “balão vazio”
                summary_text = (m.get("text") or "").strip()
                if not summary_text:
                    # fallback: junta bullets (se existirem) numa frase única
                    bullets = m.get("bullets", []) or []
                    summary_text = " ".join([str(b).strip() for b in bullets if str(b).strip()])

                html = (
                    f"<div class='bubble'>"
                    f"  <div class='bubble-title'>Resumo geral</div>"
                    f"  <div class='bubble-muted'>{escape(summary_text)}</div>"
                    f"</div>"
                )
                st.markdown(html, unsafe_allow_html=True)

            elif m.get("type") == "findings":
                intro = (m.get("intro") or "Aqui vai uma resposta objetiva baseada nos dados:").strip()
                parts = [
                    "<div class='bubble'>",
                    f"<div class='bubble-muted'>{escape(intro)}</div>",
                ]
                for it in (m.get("findings", []) or [])[:10]:
                    title = escape(str(it.get("title", "Insight")))
                    text = escape(str(it.get("text", "")))
                    parts.append(
                        "<div class='finding-card'>"
                        f"<div class='finding-title'>{title}</div>"
                        f"<div class='finding-text'>{text}</div>"
                        "</div>"
                    )
                parts.append("</div>")
                st.markdown("\n".join(parts), unsafe_allow_html=True)

                with st.expander("SQL usada (debug)"):
                    st.code(m.get("sql") or "", language="sql")
            else:
                st.markdown(escape(m.get("text", "")))

# ---------------- COMPOSER (fixo) ----------------
def on_clear():
    st.session_state.messages = []
    st.session_state.pending_job = None
    st.session_state["composer_input"] = ""

def on_send():
    q = (st.session_state.get("composer_input") or "").strip()
    if not q:
        return
    _enqueue_user_question(source, q)
    st.session_state["composer_input"] = ""  # OK dentro do callback

st.markdown('<div class="composer"><div class="composer-inner">', unsafe_allow_html=True)
col_a, col_b, col_c = st.columns([0.76, 0.14, 0.10])

with col_a:
    st.text_area(
        "Pergunta",
        label_visibility="collapsed",
        height=52,
        placeholder="Pergunta sobre o relatório… ex.: O que devo priorizar este mês?",
        key="composer_input",
    )

with col_b:
    st.markdown('<div class="send-btn">', unsafe_allow_html=True)
    st.button("Enviar", use_container_width=True, on_click=on_send)
    st.markdown("</div>", unsafe_allow_html=True)

with col_c:
    st.button("Limpar", use_container_width=True, on_click=on_clear)

st.markdown("</div></div>", unsafe_allow_html=True)

# ---------------- PROCESS PENDING JOB ----------------
if st.session_state.pending_job is not None:
    job = st.session_state.pending_job
    try:
        current_source = job["source"]
        q_user = job["question"]

        # ---------------- BigQuery (Rubis Gas) ----------------
        if current_source.startswith("Rubis Gas"):
            if not bq:
                raise RuntimeError("BigQuery não inicializado. Verifique GOOGLE_APPLICATION_CREDENTIALS_JSON.")

            active_table, table_kind = _active_bq_table_and_kind(current_source)
            if not active_table:
                raise RuntimeError("Nenhuma tabela/view configurada. Defina BQ_VIEW_* ou BQ_TABLE.")

            try:
                schema_cols = get_table_schema(active_table)
            except Forbidden as e:
                raise RuntimeError(
                    "A Service Account não tem permissão para ler o schema da VIEW. "
                    "Garanta no mínimo: bigquery.tables.get e bigquery.jobs.create. "
                    f"Detalhe: {e}"
                )
            except NotFound:
                raise RuntimeError(f"A VIEW não existe. Confirme o nome: {active_table}")

            sql = build_sql_with_ai(q_user, active_table, schema_cols, table_kind)
            if not sql or not sql_is_safe(sql, active_table):
                findings = [{"title": "Consulta inválida", "text": "Não foi possível gerar uma SQL segura. Refine a pergunta."}]
                sql_used = sql or ""
                df = pd.DataFrame()
            else:
                sql = ensure_limit(sql)
                try:
                    df = bq.query(sql).result().to_dataframe()
                except Forbidden as e:
                    raise RuntimeError(
                        "Permissão insuficiente para executar queries. "
                        "Garanta: bigquery.jobs.create no projeto e acesso de leitura nas tabelas base usadas pela VIEW. "
                        f"Detalhe: {e}"
                    )
                except BadRequest as e:
                    raise RuntimeError(f"SQL inválida gerada. Detalhe: {e}")

                findings = ai_key_findings(q_user, df, sql, n=6)
                sql_used = sql

        # ---------------- Instagram ----------------
        elif current_source.startswith("Instagram"):
            ig = instagram_adapter_from_env()

            fields_env = os.getenv("IGI_FIELDS", "month,followers_count,follows_count").split(",")
            has_month = any(f.strip().lower() == "month" for f in fields_env)
            gran = None if has_month else "day"

            drt = (os.getenv("IGI_DATE_RANGE_TYPE") or "").strip() or None
            if drt:
                df = ig.query(fields=[f.strip() for f in fields_env], date_range_type=drt, time_granularity=gran)
                sql_used = f"Supermetrics IGI fields={','.join([f.strip() for f in fields_env])} range={drt}"
            else:
                end = date.today()
                start = end - timedelta(days=30)
                df = ig.query(
                    fields=[f.strip() for f in fields_env],
                    date_from=start.isoformat(),
                    date_to=end.isoformat(),
                    time_granularity=gran,
                )
                sql_used = f"Supermetrics IGI fields={','.join([f.strip() for f in fields_env])} {start}..{end}"

            findings = ai_key_findings(q_user, df, sql_used, n=6)

        # ---------------- Facebook ----------------
        elif current_source.startswith("Facebook"):
            fb = facebook_pages_adapter_from_env()
            end = date.today()
            start = end - timedelta(days=30)
            fields = os.getenv(
                "FPI_FIELDS",
                "date,page_id,post_id,permalink,post_type,message,post_reach,post_impressions,post_engaged_users,reactions_total,comments,shares,link_clicks,video_views",
            ).split(",")

            df = fb.query(
                fields=[f.strip() for f in fields],
                date_from=start.isoformat(),
                date_to=end.isoformat(),
                time_granularity="day",
            )
            sql_used = f"Supermetrics FB fields={','.join(fields)} {start}..{end}"
            findings = ai_key_findings(q_user, df, sql_used, n=6)

        else:
            raise RuntimeError("Fonte não suportada. Selecione uma fonte válida.")

        # --- Persist results as messages ---
        if job["kind"] == "summary":
            bullets = []
            for it in findings[:5]:
                t = (it.get("title") or "").strip()
                x = (it.get("text") or "").strip()
                if t and x:
                    bullets.append(f"{t}: {x}")
                elif x:
                    bullets.append(x)

            # Resumo em formato de mini-texto (contextual), sem bullets na UI.
            # Mantemos bullets só como fallback/debug.
            sentences = []
            for b in bullets:
                b = str(b).strip()
                if not b:
                    continue
                if ":" in b:
                    sentences.append(b.split(":", 1)[1].strip())
                else:
                    sentences.append(b)
            summary_text = " ".join(sentences).strip()
            if summary_text:
                summary_text = "Resumo do período: " + summary_text
            msg = {
                "role": "assistant",
                "type": "summary",
                "text": summary_text,
                "bullets": bullets,
                "ts": time.time(),
            }
            st.session_state.summary_cached[current_source] = msg
            st.session_state.messages.append(msg)
        else:
            st.session_state.messages.append({
                "role": "assistant",
                "type": "findings",
                "intro": "Aqui vai uma resposta objetiva baseada nos dados:",
                "findings": findings,
                "sql": sql_used,
                "ts": time.time()
            })

    except Exception as e:
        st.session_state.messages.append({
            "role": "assistant",
            "type": "text",
            "text": f"Erro ao consultar: {str(e)}",
            "ts": time.time()
        })
    finally:
        st.session_state.pending_job = None
        st.rerun()
