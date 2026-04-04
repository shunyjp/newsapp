from __future__ import annotations

from typing import Any


def should_include_in_notebooklm(item: dict[str, Any], policy: dict[str, Any]) -> tuple[bool, str | None]:
    notebook_policy = policy["notebooklm"]
    body_kind = str(item.get("body_kind", ""))
    content_status = str(item.get("content_status", ""))
    quality_tier = str(item.get("quality_tier", ""))
    eligibility = str(item.get("notebooklm_eligibility", ""))

    if content_status in set(notebook_policy.get("exclude_content_statuses", [])):
        return False, "content_status_excluded"
    if body_kind in set(notebook_policy.get("exclude_body_kinds", [])):
        return False, "body_kind_excluded"
    if quality_tier in {"reject", "low"} and not notebook_policy.get("allow_low_quality_with_warning", False):
        return False, "quality_excluded"
    if eligibility == "ineligible":
        return False, "eligibility_excluded"
    if body_kind in set(notebook_policy.get("conditional_body_kinds", [])):
        return True, "conditional_body_kind"
    return True, None
