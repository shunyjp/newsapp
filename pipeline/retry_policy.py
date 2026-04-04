from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from config import CONFIG_DIR, load_structured_config


DEFAULT_RETRY_POLICY: dict[str, Any] = {
    "history_limit": 10,
    "reason_rules": {
        "analyze.retry.ineligible": {
            "mode": "any",
            "match": {
                "reader_eligibility": ["ineligible"],
                "notebooklm_eligibility": ["ineligible"],
            },
            "max_retries": 3,
            "cooldown_hours": 24,
            "source_overrides": {},
            "body_kind_overrides": {},
        },
        "analyze.retry.low_quality": {
            "mode": "any",
            "match": {
                "quality_tier": ["low"],
            },
            "max_retries": 3,
            "cooldown_hours": 24,
            "source_overrides": {},
            "body_kind_overrides": {},
        },
    }
}


def _merge_policy(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_policy(merged[key], value)
            continue
        merged[key] = deepcopy(value)
    return merged


def load_retry_policy(path: str | Path | None = None) -> dict[str, Any]:
    config_path = Path(path) if path is not None else CONFIG_DIR / "retry_policy.json"
    if not config_path.exists():
        return deepcopy(DEFAULT_RETRY_POLICY)
    loaded = load_structured_config(config_path)
    return _merge_policy(DEFAULT_RETRY_POLICY, loaded)


def normalize_retry_policy(policy: dict[str, Any] | None = None) -> dict[str, Any]:
    if policy is None:
        return deepcopy(DEFAULT_RETRY_POLICY)
    return _merge_policy(DEFAULT_RETRY_POLICY, policy)


def _resolve_reason_rule(policy: dict[str, Any], reason_code: str, item: dict[str, Any]) -> dict[str, Any]:
    rules = dict(policy.get("reason_rules", {}) or {})
    resolved = deepcopy(rules.get(reason_code, {}))
    source_rule = dict(resolved.get("source_overrides", {}).get(str(item.get("source_id", "")), {}) or {})
    if source_rule:
        resolved = _merge_policy(resolved, source_rule)
    body_kind_rule = dict(resolved.get("body_kind_overrides", {}).get(str(item.get("body_kind", "")), {}) or {})
    if body_kind_rule:
        resolved = _merge_policy(resolved, body_kind_rule)
    return resolved


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _load_retry_history(item: dict[str, Any], reason_code: str) -> list[dict[str, Any]]:
    diagnostics = dict(item.get("cleaning_diagnostics", {}) or {})
    history = dict(diagnostics.get("retry_policy_history", {}) or {})
    entries = list(history.get(reason_code, []) or [])
    return [dict(entry) for entry in entries if isinstance(entry, dict)]


def evaluate_retry_rule(
    reason_code: str,
    item: dict[str, Any],
    policy: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    active_policy = normalize_retry_policy(policy)
    rule = _resolve_reason_rule(active_policy, reason_code, item)
    history = _load_retry_history(item, reason_code)
    attempt_count = len(history)
    last_attempt_at = history[-1].get("attempted_at") if history else None
    enabled = bool(rule) and rule.get("enabled", True) is not False
    matchers = dict(rule.get("match", {}) or {})
    comparisons = [
        str(item.get(field, "")) in {str(value) for value in values}
        for field, values in matchers.items()
    ]
    mode = str(rule.get("mode", "any")).lower()
    matched = bool(comparisons) and (all(comparisons) if mode == "all" else any(comparisons))
    max_retries = rule.get("max_retries")
    cooldown_hours = int(rule.get("cooldown_hours", 0) or 0)
    blocked_reason: str | None = None
    next_retry_at: str | None = None
    reference_now = now or datetime.now(timezone.utc)

    if matched and not enabled:
        blocked_reason = "override_disabled"

    if enabled and matched and max_retries is not None and attempt_count >= int(max_retries):
        blocked_reason = "max_retries_reached"

    if enabled and matched and blocked_reason is None and cooldown_hours > 0:
        last_attempt = _parse_iso_datetime(str(last_attempt_at or ""))
        if last_attempt is not None:
            retry_after = last_attempt + timedelta(hours=cooldown_hours)
            next_retry_at = retry_after.replace(microsecond=0).isoformat()
            if reference_now < retry_after:
                blocked_reason = "cooldown_active"

    return {
        "reason_code": reason_code,
        "enabled": enabled,
        "matched": matched,
        "eligible": enabled and matched and blocked_reason is None,
        "blocked_reason": blocked_reason,
        "max_retries": None if max_retries is None else int(max_retries),
        "cooldown_hours": cooldown_hours,
        "attempt_count": attempt_count,
        "last_attempt_at": last_attempt_at,
        "next_retry_at": next_retry_at,
    }


def reason_matches_retry_policy(
    reason_code: str,
    item: dict[str, Any],
    policy: dict[str, Any] | None = None,
) -> bool:
    return bool(evaluate_retry_rule(reason_code, item, policy).get("eligible"))
