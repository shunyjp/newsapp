from __future__ import annotations

from typing import Any

from config import CONFIG_DIR, load_structured_config


def load_source_config() -> dict[str, Any]:
    return load_structured_config(CONFIG_DIR / "sources.yaml")


def source_map(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(source["source_id"]): dict(source)
        for source in config.get("sources", [])
    }


def _normalize_source_set_entry(entry: Any) -> dict[str, Any]:
    if isinstance(entry, list):
        return {
            "source_ids": [str(item) for item in entry],
            "default_max_items": None,
        }
    if isinstance(entry, dict):
        source_ids = entry.get("source_ids", [])
        return {
            **dict(entry),
            "source_ids": [str(item) for item in source_ids],
            "default_max_items": entry.get("default_max_items"),
        }
    raise ValueError(f"Invalid source set entry: {entry!r}")


def get_source_set_config(config: dict[str, Any], source_set: str) -> dict[str, Any]:
    source_sets = dict(config.get("source_sets", {}) or {})
    try:
        raw_entry = source_sets[str(source_set)]
    except KeyError as exc:
        available = ", ".join(sorted(source_sets))
        raise ValueError(
            f"Unknown source set '{source_set}'. Available source sets: {available}"
        ) from exc
    return _normalize_source_set_entry(raw_entry)


def resolve_source_ids(
    config: dict[str, Any],
    *,
    source_id: str | None = None,
    source_set: str | None = None,
) -> list[str]:
    if bool(source_id) == bool(source_set):
        raise ValueError("Provide exactly one of --source or --source-set.")
    if source_id:
        return [source_id]
    source_set_config = get_source_set_config(config, str(source_set))
    source_ids = list(source_set_config.get("source_ids", []))
    if not source_ids:
        raise ValueError(f"Source set '{source_set}' is empty.")
    return [str(item) for item in source_ids]


def resolve_collect_max_items(
    config: dict[str, Any],
    *,
    source_set: str | None,
    explicit_max_items: int | None,
    fallback_default: int,
) -> int:
    if explicit_max_items is not None:
        return explicit_max_items
    if source_set:
        source_set_config = get_source_set_config(config, source_set)
        configured = source_set_config.get("default_max_items")
        if isinstance(configured, int) and configured > 0:
            return configured
    return fallback_default
