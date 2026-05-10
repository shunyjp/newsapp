from __future__ import annotations

import os
from typing import Any

import requests


def get_n8n_headers(api_key: str | None = None) -> dict[str, str]:
    key = api_key or os.getenv("N8N_API_KEY", "")
    headers = {"Content-Type": "application/json"}
    if key:
        headers["X-N8N-API-KEY"] = key
    return headers


def check_n8n_health(base_url: str | None = None, timeout: int = 10) -> dict[str, Any]:
    url = (base_url or os.getenv("N8N_API_BASE_URL", "http://localhost:5678/api/v1")).rstrip("/")
    endpoint = f"{url}/workflows"
    response = requests.get(endpoint, headers=get_n8n_headers(), timeout=timeout)
    return {"status_code": response.status_code, "ok": response.ok, "endpoint": endpoint}
