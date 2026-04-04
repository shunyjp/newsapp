from __future__ import annotations

from typing import Any


def build_reader_warnings(item: dict[str, Any], policy: dict[str, Any]) -> list[str]:
    reader_policy = policy["reader"]
    warnings: list[str] = []
    if item.get("body_kind") in set(reader_policy.get("warning_body_kinds", [])):
        warnings.append(f"body_kind={item.get('body_kind')}")
    if item.get("quality_tier") in {"low", "reject"}:
        warnings.append(f"quality_tier={item.get('quality_tier')}")
    if item.get("content_status") == "unavailable":
        warnings.append("content_unavailable")
    if item.get("content_warning"):
        warnings.append(str(item["content_warning"]))
    return warnings


def should_include_in_reader(item: dict[str, Any], policy: dict[str, Any]) -> tuple[bool, list[str]]:
    reader_policy = policy["reader"]
    warnings = build_reader_warnings(item, policy)
    if item.get("content_status") == "unavailable" and not reader_policy.get("include_unavailable", True):
        return False, warnings
    if item.get("quality_tier") == "reject" and item.get("body_kind") != "metadata_only":
        return False, warnings
    return True, warnings
