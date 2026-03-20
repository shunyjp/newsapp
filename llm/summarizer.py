import json
import re
from typing import Any

import requests

from config import OLLAMA_MODEL, SUMMARY_RETRIES
from llm.ollama_client import generate


DEFAULT_SUMMARY = {
    "summary": "",
    "key_points": [],
    "entities": [],
    "category": [],
    "signal_score": 0.0,
}


def _build_prompt(chunk_text: str) -> str:
    schema = json.dumps(DEFAULT_SUMMARY, ensure_ascii=False)
    return (
        "You are extracting structured intelligence from a news transcript chunk.\n"
        "Return JSON only. No markdown. No commentary.\n"
        "Rules:\n"
        "- Use the provided schema exactly.\n"
        "- key_points must contain concise factual bullet-style strings.\n"
        "- entities must contain notable people, companies, products, or countries.\n"
        "- category must contain one or more topical labels.\n"
        "- signal_score must be a float between 0.0 and 1.0.\n"
        "- If information is missing, return empty strings or empty arrays.\n\n"
        f"Schema:\n{schema}\n\n"
        f"Chunk:\n{chunk_text}"
    )


def _coerce_summary(payload: dict[str, Any]) -> dict[str, Any]:
    result = dict(DEFAULT_SUMMARY)
    result["summary"] = str(payload.get("summary", "")).strip()
    result["key_points"] = [
        str(item).strip() for item in payload.get("key_points", []) if str(item).strip()
    ]
    result["entities"] = [
        str(item).strip() for item in payload.get("entities", []) if str(item).strip()
    ]
    result["category"] = [
        str(item).strip() for item in payload.get("category", []) if str(item).strip()
    ]
    try:
        signal_score = float(payload.get("signal_score", 0.0))
    except (TypeError, ValueError):
        signal_score = 0.0
    result["signal_score"] = max(0.0, min(1.0, signal_score))
    return result


def _extract_json(raw_text: str) -> dict[str, Any]:
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", raw_text, re.DOTALL)
        if not match:
            raise
        return json.loads(match.group(0))


def summarize_chunk(chunk_text: str, model: str = OLLAMA_MODEL) -> dict[str, Any]:
    last_error: Exception | None = None
    prompt = _build_prompt(chunk_text)

    for _ in range(SUMMARY_RETRIES):
        try:
            raw_response = generate(prompt, model=model)
            parsed = _extract_json(raw_response)
            return _coerce_summary(parsed)
        except (json.JSONDecodeError, ValueError, requests.RequestException) as exc:
            last_error = exc

    return dict(DEFAULT_SUMMARY)
