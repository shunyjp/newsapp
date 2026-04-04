from __future__ import annotations

from typing import Any

from db.database import Database
from db.repository import ItemRepository
from normalization.noise_rules import is_explicit_noise_title


def find_explicit_noise_items(
    repository: ItemRepository,
    *,
    source_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for item in repository.list_items():
        if source_ids is not None and str(item.get("source_id", "")) not in source_ids:
            continue
        if not is_explicit_noise_title(str(item.get("title", "") or "")):
            continue
        matches.append(
            {
                "item_id": str(item.get("item_id", "")),
                "source_id": str(item.get("source_id", "")),
                "title": str(item.get("title", "")),
                "url": str(item.get("url", "")),
                "body_kind": str(item.get("body_kind", "")),
                "content_status": str(item.get("content_status", "")),
                "quality_tier": str(item.get("quality_tier", "")),
            }
        )
    return matches


def cleanup_explicit_noise_items(
    db: Database,
    *,
    source_ids: set[str] | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    repository = ItemRepository(db)
    matches = find_explicit_noise_items(repository, source_ids=source_ids)
    deleted_count = 0
    if matches and not dry_run:
        deleted_count = repository.delete_items([item["item_id"] for item in matches])
    return {
        "dry_run": dry_run,
        "matched_count": len(matches),
        "deleted_count": deleted_count,
        "matched_items": matches,
    }
