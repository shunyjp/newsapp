from __future__ import annotations

import re
from typing import Any


MOJIBAKE_PATTERN = re.compile(r"[�\ufffd]")
URL_PATTERN = re.compile(r"https?://\S+|www\.\S+", flags=re.IGNORECASE)


def _ratio(count: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return count / total


def _count_cta_terms(text: str, terms: list[str]) -> int:
    lowered = text.lower()
    return sum(lowered.count(term.lower()) for term in terms)


def evaluate_quality(item: dict[str, Any], policy: dict[str, Any]) -> dict[str, Any]:
    cleaned_text = str(item.get("cleaned_text", "") or "")
    diagnostics = item.get("retrieval_diagnostics", {}) or {}
    quality_policy = policy["quality"]
    text_length = len(cleaned_text)
    mojibake_ratio = _ratio(len(MOJIBAKE_PATTERN.findall(cleaned_text)), max(1, text_length))
    url_ratio = _ratio(len(URL_PATTERN.findall(cleaned_text)), max(1, len(cleaned_text.split()) or 1))
    cta_ratio = _ratio(
        _count_cta_terms(cleaned_text, list(quality_policy.get("cta_terms", []))),
        max(1, len(cleaned_text.split()) or 1),
    )

    reasons: list[str] = []
    quality_tier = "high"
    if item.get("content_status") != "available":
        quality_tier = "reject"
        reasons.append("content_unavailable")
    elif item.get("body_kind") == "metadata_only":
        quality_tier = "reject"
        reasons.append("metadata_only")
    elif diagnostics.get("failure_reason"):
        quality_tier = "low"
        reasons.append("retrieval_warning")
    elif text_length < int(quality_policy["minimum_cleaned_text_length"]):
        quality_tier = "low"
        reasons.append("too_short")
    elif item.get("body_kind") == "description_only":
        quality_tier = "medium"
        reasons.append("description_only")

    if mojibake_ratio > float(quality_policy["max_mojibake_ratio"]):
        quality_tier = "reject"
        reasons.append("mojibake")
    elif url_ratio > float(quality_policy["max_url_ratio"]) or cta_ratio > float(quality_policy["max_cta_ratio"]):
        if quality_tier not in {"reject", "low"}:
            quality_tier = "low"
        reasons.append("promotional_noise")
    elif (
        quality_tier == "high"
        and text_length < int(quality_policy["warning_cleaned_text_length"])
    ):
        quality_tier = "medium"
        reasons.append("limited_detail")

    reader_eligibility = "eligible"
    notebooklm_eligibility = "eligible"

    if quality_tier == "reject":
        reader_eligibility = "eligible_with_warning" if item.get("body_kind") == "metadata_only" else "ineligible"
        notebooklm_eligibility = "ineligible"
    elif quality_tier == "low":
        reader_eligibility = "eligible_with_warning"
        notebooklm_eligibility = (
            "eligible_with_warning"
            if item.get("body_kind") == "description_only"
            else "ineligible"
        )
    elif item.get("body_kind") == "description_only":
        reader_eligibility = "eligible_with_warning"
        notebooklm_eligibility = "eligible_with_warning"

    return {
        "quality_tier": quality_tier,
        "reader_eligibility": reader_eligibility,
        "notebooklm_eligibility": notebooklm_eligibility,
        "quality_reasons": reasons,
        "metrics": {
            "cleaned_text_length": text_length,
            "mojibake_ratio": mojibake_ratio,
            "url_ratio": url_ratio,
            "cta_ratio": cta_ratio,
        },
    }
