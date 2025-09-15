# supermetrics_adapter.py
from __future__ import annotations

import os
import json
from typing import Dict, List, Optional, Any

import requests
import pandas as pd


DEFAULT_BASE_URL = os.getenv(
    "SUPERMETRICS_BASE_URL",
    "https://api.supermetrics.com/enterprise/v2/query/data/json",
)


class SupermetricsError(Exception):
    """Erro de alto nível para chamadas ao Supermetrics."""
    pass


def _read_api_key_from_env() -> str:
    """Lê a API key do ambiente, com fallbacks e saneamento."""
    key = (
        os.getenv("SUPERMETRICS_API_KEY")
        or os.getenv("SUPERMETRICS_TOKEN")
        or os.getenv("SUPERMETRICS_KEY")
        or ""
    )
    # remove espaços e aspas acidentais
    return key.strip().strip('"').strip("'")


class SupermetricsAdapter:
    """
    Adapter genérico para a Product API do Supermetrics (Enterprise v2).

    - Aceita `ds_id` (ex.: IGI para Instagram, FBI/FPI para Facebook Pages)
    - `ds_accounts`: lista de IDs (string) das contas de origem
    - `ds_user`: identificador do usuário/tenant no Supermetrics
    - A API key é enviada em *dois formatos*: querystring (api_key) e header Authorization
    """

    def __init__(
        self,
        api_key: str,
        ds_id: str,
        ds_user: str,
        ds_accounts: List[str],
        base_url: str = DEFAULT_BASE_URL,
        timeout: int = 60,
    ):
        self.api_key = (api_key or "").strip().strip('"').strip("'")
        self.ds_id = ds_id.strip()
        self.ds_user = ds_user.strip()
        self.ds_accounts = [a.strip() for a in ds_accounts if a and a.strip()]
        self.base_url = base_url
        self.timeout = timeout

        if not self.api_key:
            raise SupermetricsError("API key vazia. Configure SUPERMETRICS_API_KEY.")
        if not self.ds_id:
            raise SupermetricsError("ds_id vazio.")
        if not self.ds_user:
            raise SupermetricsError("ds_user vazio.")
        if not self.ds_accounts:
            raise SupermetricsError("ds_accounts vazio.")

    # ------------------------- HTTP ------------------------- #
    def _request_page(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Faz uma chamada GET, com:
          - `json` (payload) + `api_key` na querystring
          - `Authorization: Bearer <api_key>` no header
        """
        params = {
            "json": json.dumps(payload, separators=(",", ":")),
            "api_key": self.api_key,  # 1) forma suportada
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",  # 2) forma suportada
            "Accept": "application/json",
        }

        r = requests.get(self.base_url, params=params, headers=headers, timeout=self.timeout)
        if r.status_code != 200:
            # Propaga a mensagem da API para facilitar o diagnóstico
            raise SupermetricsError(f"HTTP {r.status_code}: {r.text[:1000]}")

        try:
            data = r.json()
        except Exception:
            raise SupermetricsError("Falha ao decodificar JSON de resposta do Supermetrics.")

        # Alguns retornos vêm com meta/error
        if isinstance(data, dict) and data.get("status") == "error":
            msg = data.get("message") or data.get("error") or "Erro no conector"
            raise SupermetricsError(str(msg))

        return data

    # ------------------------- Parsing ------------------------- #
    def _rows_to_df(self, resp: Dict[str, Any]) -> pd.DataFrame:
        """
        Converte a resposta da API em DataFrame. A API pode retornar:
          - meta.fields + data (lista de listas)  OU
          - fields + data/rows (lista de dicts)
        """
        fields_meta = (
            resp.get("fields")
            or resp.get("meta", {}).get("fields")
            or []
        )
        col_names: List[str] = []
        for f in fields_meta:
            # Alguns objetos vêm com 'id', outros com 'name'/'label'
            col_names.append(f.get("id") or f.get("name") or f.get("label") or "col")

        data = resp.get("data") or resp.get("rows") or []

        if not data:
            # Retorna DF vazio com as colunas detectadas (se houver)
            return pd.DataFrame(columns=col_names or None)

        # data pode ser lista de listas ou lista de dicts
        first = data[0]
        if isinstance(first, list):
            # caso "array de arrays"
            return pd.DataFrame(data, columns=col_names or None)

        if isinstance(first, dict):
            # caso "array de dicts"
            if not col_names:
                # se não achamos fields, infere colunas pela união das chaves
                keys = set()
                for row in data:
                    keys.update(row.keys())
                col_names = list(keys)
            rows = [[row.get(c) for c in col_names] for row in data]
            return pd.DataFrame(rows, columns=col_names)

        # fallback genérico
        return pd.DataFrame(data)

    # ------------------------- Query pública ------------------------- #
    def query(
        self,
        fields: List[str],
        date_from: Optional[str] = None,         # "YYYY-MM-DD"
        date_to: Optional[str] = None,           # "YYYY-MM-DD"
        date_range_type: Optional[str] = None,   # ex.: "last_30_days", "yesterday"
        filters: Optional[Dict[str, Any]] = None,
        max_rows: int = 10000,
        time_granularity: Optional[str] = None,  # ex.: "day"
    ) -> pd.DataFrame:
        """
        Executa uma consulta e consolida a paginação automaticamente.
        """
        payload: Dict[str, Any] = {
            "ds_id": self.ds_id,
            "ds_accounts": ",".join(self.ds_accounts),
            "ds_user": self.ds_user,
            "max_rows": max_rows,
            "fields": [f.strip() for f in fields if f and f.strip()],
        }

        if date_range_type:
            payload["date_range_type"] = date_range_type
        else:
            if date_from:
                payload["date_from"] = date_from
            if date_to:
                payload["date_to"] = date_to

        if time_granularity:
            payload["time_granularity"] = time_granularity

        if filters:
            payload["filters"] = filters

        # primeira página
        resp = self._request_page(payload)
        df = self._rows_to_df(resp)

        # paginação (next_page_params)
        next_params = (
            resp.get("meta", {}).get("next_page_params")
            or resp.get("next_page_params")
        )
        while next_params:
            payload.update(next_params)
            resp_next = self._request_page(payload)
            df_next = self._rows_to_df(resp_next)
            if not df_next.empty:
                df = pd.concat([df, df_next], ignore_index=True)
            next_params = (
                resp_next.get("meta", {}).get("next_page_params")
                or resp_next.get("next_page_params")
            )

        return df


# ------------------------- Helpers por conector ------------------------- #
def instagram_adapter_from_env() -> SupermetricsAdapter:
    """
    Constrói um adapter para Instagram Insights (IGI) a partir de ENVs:

      SUPERMETRICS_API_KEY  (obrig.)
      SUPERMETRICS_USER     (obrig.) -> ds_user
      IGI_ACCOUNTS          (obrig.) -> ids separados por vírgula
      IGI_DS_ID             (opcional, default "IGI")
    """
    api_key = _read_api_key_from_env()
    ds_user = (os.getenv("SUPERMETRICS_USER") or "").strip()
    accounts = (os.getenv("IGI_ACCOUNTS") or "").strip()

    if not api_key:
        raise SupermetricsError("SUPERMETRICS_API_KEY não configurada.")
    if not ds_user:
        raise SupermetricsError("SUPERMETRICS_USER não configurada.")
    if not accounts:
        raise SupermetricsError("IGI_ACCOUNTS não configurada.")

    ds_id = os.getenv("IGI_DS_ID", "IGI").strip()
    return SupermetricsAdapter(api_key, ds_id, ds_user, accounts.split(","))


def facebook_pages_adapter_from_env() -> SupermetricsAdapter:
    """
    Constrói um adapter para Facebook Pages (FBI/FPI) a partir de ENVs:

      SUPERMETRICS_API_KEY  (obrig.)
      SUPERMETRICS_USER     (obrig.) -> ds_user
      FPI_ACCOUNTS          (obrig.) -> ids separados por vírgula
      FPI_DS_ID             (opcional; default "FBI", troque para "FPI" se for seu conector)
    """
    api_key = _read_api_key_from_env()
    ds_user = (os.getenv("SUPERMETRICS_USER") or "").strip()
    accounts = (os.getenv("FPI_ACCOUNTS") or "").strip()

    if not api_key:
        raise SupermetricsError("SUPERMETRICS_API_KEY não configurada.")
    if not ds_user:
        raise SupermetricsError("SUPERMETRICS_USER não configurada.")
    if not accounts:
        raise SupermetricsError("FPI_ACCOUNTS não configurada.")

    ds_id = os.getenv("FPI_DS_ID", "FBI").strip()  # mude para "FPI" se a sua licença usar esse id
    return SupermetricsAdapter(api_key, ds_id, ds_user, accounts.split(","))
