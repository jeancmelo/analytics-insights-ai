# supermetrics_adapter.py
from __future__ import annotations
import os, json
from typing import Dict, List, Optional, Any
import requests
import pandas as pd

DEFAULT_BASE_URL = os.getenv(
    "SUPERMETRICS_BASE_URL",
    "https://api.supermetrics.com/enterprise/v2/query/data/json",
)

class SupermetricsError(Exception):
    pass

class SupermetricsAdapter:
    """
    Adapter genérico p/ Supermetrics Enterprise v2.
    Funciona para IGI (Instagram) e FB Pages (FBI/FPI) – ajuste ds_id e fields por ENV.
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
        self.api_key = api_key
        self.ds_id = ds_id
        self.ds_user = ds_user
        self.ds_accounts = ds_accounts
        self.base_url = base_url
        self.timeout = timeout

    def _request_page(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        params = {
            "json": json.dumps(payload, separators=(",", ":")),
            "api_key": self.api_key,
        }
        r = requests.get(self.base_url, params=params, timeout=self.timeout)
        if r.status_code != 200:
            raise SupermetricsError(f"HTTP {r.status_code}: {r.text[:500]}")
        data = r.json()
        if isinstance(data, dict) and data.get("status") == "error":
            raise SupermetricsError(data.get("message") or "Erro no conector")
        return data

    def _rows_to_df(self, resp: Dict[str, Any]) -> pd.DataFrame:
        fields_meta = resp.get("fields") or resp.get("meta", {}).get("fields") or []
        col_names = [(f.get("id") or f.get("name") or f.get("label") or "col") for f in fields_meta]
        data = resp.get("data") or resp.get("rows") or []
        if not data:
            return pd.DataFrame(columns=col_names)
        if isinstance(data[0], list):  # array de arrays
            return pd.DataFrame(data, columns=col_names)
        if isinstance(data[0], dict):  # array de dicts
            rows = [[row.get(c) for c in col_names] for row in data]
            return pd.DataFrame(rows, columns=col_names)
        return pd.DataFrame(data)

    def query(
        self,
        fields: List[str],
        date_from: Optional[str] = None,  # "YYYY-MM-DD"
        date_to: Optional[str] = None,
        date_range_type: Optional[str] = None,  # ex.: "last_30_days"
        filters: Optional[Dict[str, Any]] = None,
        max_rows: int = 10000,
        time_granularity: Optional[str] = None,  # ex.: "day"
    ) -> pd.DataFrame:
        payload: Dict[str, Any] = {
            "ds_id": self.ds_id,
            "ds_accounts": ",".join(self.ds_accounts),
            "ds_user": self.ds_user,
            "max_rows": max_rows,
            "fields": fields,
        }
        if date_range_type:
            payload["date_range_type"] = date_range_type
        else:
            if date_from: payload["date_from"] = date_from
            if date_to:   payload["date_to"] = date_to
        if time_granularity:
            payload["time_granularity"] = time_granularity
        if filters:
            payload["filters"] = filters

        resp = self._request_page(payload)
        df = self._rows_to_df(resp)

        next_params = resp.get("meta", {}).get("next_page_params") or resp.get("next_page_params")
        while next_params:
            payload.update(next_params)
            resp_next = self._request_page(payload)
            df_next = self._rows_to_df(resp_next)
            if not df_next.empty:
                df = pd.concat([df, df_next], ignore_index=True)
            next_params = resp_next.get("meta", {}).get("next_page_params") or resp_next.get("next_page_params")
        return df


# ---------- Helpers p/ construir adapters a partir de ENVs ----------

def instagram_adapter_from_env() -> SupermetricsAdapter:
    """
    ENVs obrigatórios:
      SUPERMETRICS_API_KEY
      SUPERMETRICS_USER          -> ds_user
      IGI_ACCOUNTS               -> ids, separados por vírgula
    ENVs opcionais:
      IGI_DS_ID (default "IGI")
    """
    api_key = os.environ["SUPERMETRICS_API_KEY"]
    ds_user = os.environ["SUPERMETRICS_USER"]
    accounts = os.environ["IGI_ACCOUNTS"].split(",")
    ds_id = os.getenv("IGI_DS_ID", "IGI")
    return SupermetricsAdapter(api_key, ds_id, ds_user, accounts)

def facebook_pages_adapter_from_env() -> SupermetricsAdapter:
    """
    ENVs obrigatórios:
      SUPERMETRICS_API_KEY
      SUPERMETRICS_USER
      FPI_ACCOUNTS               -> ids, separados por vírgula
    ENVs opcionais:
      FPI_DS_ID (default "FBI" — troque p/ "FPI" se a sua licença usar esse id)
    """
    api_key = os.environ["SUPERMETRICS_API_KEY"]
    ds_user = os.environ["SUPERMETRICS_USER"]
    accounts = os.environ["FPI_ACCOUNTS"].split(",")
    ds_id = os.getenv("FPI_DS_ID", "FBI")
    return SupermetricsAdapter(api_key, ds_id, ds_user, accounts)
