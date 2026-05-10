from __future__ import annotations

import os
from typing import Any

import requests


def get_dify_headers(api_key: str | None = None) -> dict[str, str]:
    key = api_key or os.getenv("DIFY_API_KEY", "")
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Authorization"] = f"Bearer {key}"
    return headers


def check_dify_health(base_url: str | None = None, timeout: int = 10) -> dict[str, Any]:
    url = (base_url or os.getenv("DIFY_API_BASE_URL", "http://localhost/v1")).rstrip("/")
    endpoint = f"{url}/workflows"
    response = requests.get(endpoint, headers=get_dify_headers(), timeout=timeout)
    return {"status_code": response.status_code, "ok": response.ok, "endpoint": endpoint}
