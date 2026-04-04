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
    source_sets = dict(config.get("source_sets", {}) or {})
    try:
        source_ids = list(source_sets[str(source_set)])
    except KeyError as exc:
        available = ", ".join(sorted(source_sets))
        raise ValueError(
            f"Unknown source set '{source_set}'. Available source sets: {available}"
        ) from exc
    if not source_ids:
        raise ValueError(f"Source set '{source_set}' is empty.")
    return [str(item) for item in source_ids]
