import streamlit as st

# Permite embed no Looker Studio (injeta CSP e remove X-Frame-Options)
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

st.set_page_config(page_title="MVP Q&A", layout="wide")
st.title("MVP – Q&A com OpenAI")
st.write("Se você está vendo isso no Looker Studio, o embed funcionou ✅")

# ---- demo simples (sem chamar OpenAI de verdade) ----
pergunta = st.text_input("Faça uma pergunta:")
if pergunta:
    st.info("Aqui você chamaria a OpenAI ou sua lógica de Q&A sobre os dados.")
