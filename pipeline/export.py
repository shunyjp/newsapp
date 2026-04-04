from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from config import CONFIG_DIR, load_structured_config
from db.database import Database
from db.repository import ItemRepository
from evaluation.notebooklm_policy import should_include_in_notebooklm
from evaluation.quality import evaluate_quality
from evaluation.reader_policy import should_include_in_reader
from outputs.export_notebooklm import export_notebooklm_json, export_notebooklm_markdown
from outputs.export_reader import export_reader_json, export_reader_markdown
from pipeline.migrate import map_legacy_video_to_item
from pipeline.report_codes import describe_reason


@dataclass(slots=True)
class ExportCompareReport:
    items_priority_count: int
    legacy_fallback_count: int
    overlapping_video_count: int
    items_priority_only_count: int
    legacy_only_count: int
    changed_video_count: int
    items_priority_source_counts: dict[str, int]
    legacy_fallback_source_counts: dict[str, int]
    source_count_diff: dict[str, int]
    items_priority_exclusion_reasons: dict[str, int]
    legacy_exclusion_reasons: dict[str, int]
    exclusion_reason_diff: dict[str, int]


def _increment(counter: dict[str, int], key: str) -> None:
    normalized = key or "unknown"
    counter[normalized] = counter.get(normalized, 0) + 1


def _normalize_export_reason(code: str) -> str:
    if code.startswith("export.exclusion."):
        return code
    return f"export.exclusion.{code}"


def _reason_entry(code: str) -> dict[str, str]:
    normalized = _normalize_export_reason(code)
    return {"code": normalized, "message": describe_reason(normalized)}


def _load_chunk_rows(repository: ItemRepository, item_id: str) -> list[dict[str, Any]]:
    chunks = repository.get_item_chunks(item_id)
    summaries = repository.get_item_chunk_summaries(item_id)
    rows: list[dict[str, Any]] = []
    for chunk in chunks:
        summary = summaries.get(chunk["chunk_id"], {})
        rows.append(
            {
                "chunk_id": chunk["chunk_id"],
                "chunk_no": chunk["chunk_no"],
                "text": chunk["text"],
                "summary": summary.get("summary", ""),
                "key_points": summary.get("key_points", []),
                "entities": summary.get("entities", []),
                "category": summary.get("category", []),
                "signal_score": summary.get("signal_score", 0.0),
            }
        )
    return rows


def _to_export_item(repository: ItemRepository, item: dict[str, Any], warnings: list[str]) -> dict[str, Any]:
    summary = repository.get_item_summary(item["item_id"]) or {}
    chunk_rows = _load_chunk_rows(repository, item["item_id"])
    key_points: list[str] = []
    entities: list[str] = []
    categories: list[str] = []
    for chunk in chunk_rows:
        key_points.extend([point for point in chunk.get("key_points", []) if point not in key_points])
        entities.extend([entity for entity in chunk.get("entities", []) if entity not in entities])
        categories.extend([category for category in chunk.get("category", []) if category not in categories])
    why_it_matters = key_points[0] if key_points else (summary.get("short_summary") or item.get("content_warning") or "")
    signal_score = max([float(chunk.get("signal_score", 0.0)) for chunk in chunk_rows], default=0.0)
    return {
        "video_id": item["external_id"] or item["item_id"],
        "item_id": item["item_id"],
        "title": item.get("title", ""),
        "channel": item.get("author", ""),
        "published_at": item.get("published_at", ""),
        "url": item["url"],
        "transcript_source": item.get("retrieval_diagnostics", {}).get("selected_caption_source", ""),
        "transcript_length": len(item.get("raw_text", "") or ""),
        "content_status": item.get("content_status", "available"),
        "content_warning": item.get("content_warning", ""),
        "body_kind": item.get("body_kind", ""),
        "quality_tier": item.get("quality_tier", ""),
        "reader_warning_flags": warnings,
        "reader_warning_details": [_reason_entry(code) for code in warnings],
        "signal_score": signal_score,
        "short_summary": summary.get("short_summary", "No summary available."),
        "detailed_summary": summary.get("detailed_summary", "No detailed summary available."),
        "reader_points": key_points[:5],
        "why_it_matters": why_it_matters,
        "aggregated_key_points": key_points,
        "aggregated_entities": entities,
        "aggregated_categories": categories,
        "chunk_summaries": chunk_rows,
        "cleaned_text": item.get("cleaned_text", ""),
    }


def _to_export_item_from_legacy(legacy_video: dict[str, Any], warnings: list[str]) -> dict[str, Any]:
    chunk_rows: list[dict[str, Any]] = []
    key_points: list[str] = []
    entities: list[str] = []
    categories: list[str] = []
    chunk_summaries = dict(legacy_video.get("chunk_summaries", {}))
    for chunk in legacy_video.get("chunks", []):
        summary = chunk_summaries.get(chunk["chunk_id"], {})
        row = {
            "chunk_id": chunk["chunk_id"],
            "chunk_no": chunk["chunk_no"],
            "text": chunk["text"],
            "summary": summary.get("summary", ""),
            "key_points": summary.get("key_points", []),
            "entities": summary.get("entities", []),
            "category": summary.get("category", []),
            "signal_score": summary.get("signal_score", 0.0),
        }
        chunk_rows.append(row)
        for point in row["key_points"]:
            if point not in key_points:
                key_points.append(point)
        for entity in row["entities"]:
            if entity not in entities:
                entities.append(entity)
        for category in row["category"]:
            if category not in categories:
                categories.append(category)
    signal_score = max([float(chunk.get("signal_score", 0.0)) for chunk in chunk_rows], default=0.0)
    return {
        "video_id": legacy_video["video_id"],
        "item_id": f"legacy:{legacy_video['video_id']}",
        "title": legacy_video.get("title", ""),
        "channel": legacy_video.get("channel", ""),
        "published_at": legacy_video.get("published_at", ""),
        "url": legacy_video.get("url", ""),
        "transcript_source": legacy_video.get("transcript_source", ""),
        "transcript_length": int(legacy_video.get("transcript_length", 0) or 0),
        "content_status": legacy_video.get("content_status", "available"),
        "content_warning": legacy_video.get("content_warning", ""),
        "body_kind": legacy_video.get("body_kind", ""),
        "quality_tier": legacy_video.get("quality_tier", ""),
        "reader_warning_flags": warnings,
        "reader_warning_details": [_reason_entry(code) for code in warnings],
        "signal_score": signal_score,
        "short_summary": legacy_video.get("short_summary", "No summary available."),
        "detailed_summary": legacy_video.get("detailed_summary", "No detailed summary available."),
        "reader_points": key_points[:5],
        "why_it_matters": key_points[0] if key_points else legacy_video.get("content_warning", ""),
        "aggregated_key_points": key_points,
        "aggregated_entities": entities,
        "aggregated_categories": categories,
        "chunk_summaries": chunk_rows,
        "cleaned_text": legacy_video.get("cleaned_text", ""),
    }


def export_items(
    db: Database,
    export_format: str,
    output_dir,
    query: str | None = None,
    compare: bool = False,
    source_ids: set[str] | None = None,
):
    repository = ItemRepository(db)
    policy = load_structured_config(CONFIG_DIR / "policies.yaml")
    items = repository.list_items()
    export_rows: list[dict[str, Any]] = []
    items_priority_exclusion_reasons: dict[str, int] = {}
    items_priority_source_counts: dict[str, int] = {}
    seen_external_ids = {
        str(item.get("external_id", "") or "")
        for item in items
        if item.get("external_id")
    }
    for item in items:
        if source_ids is not None and str(item.get("source_id", "")) not in source_ids:
            continue
        if export_format.startswith("notebooklm"):
            include, reason = should_include_in_notebooklm(item, policy)
            if not include:
                if compare:
                    _increment(items_priority_exclusion_reasons, _normalize_export_reason(reason or "excluded"))
                continue
            warnings = [reason] if reason else []
        else:
            include, warnings = should_include_in_reader(item, policy)
            if not include:
                if compare:
                    for warning in warnings or ["excluded"]:
                        _increment(items_priority_exclusion_reasons, _normalize_export_reason(warning))
                continue
        export_rows.append(_to_export_item(repository, item, warnings))
        if compare:
            _increment(items_priority_source_counts, str(item.get("source_id", "") or "unknown"))

    legacy_exclusion_reasons: dict[str, int] = {}
    legacy_only_rows: list[dict[str, Any]] = []
    legacy_fallback_source_counts: dict[str, int] = {}
    for legacy_video in db.list_legacy_videos():
        if source_ids is not None and "youtube.default" not in source_ids:
            continue
        if str(legacy_video.get("video_id", "")) in seen_external_ids:
            continue
        canonical_item, migration_warnings = map_legacy_video_to_item(legacy_video)
        candidate = {
            "item_id": canonical_item.item_id,
            "external_id": canonical_item.external_id,
            "title": canonical_item.title,
            "author": canonical_item.author,
            "published_at": canonical_item.published_at,
            "url": canonical_item.url,
            "raw_text": canonical_item.raw_text,
            "cleaned_text": canonical_item.cleaned_text,
            "body_kind": canonical_item.body_kind,
            "content_status": canonical_item.content_status,
            "content_warning": canonical_item.content_warning,
            "retrieval_diagnostics": canonical_item.retrieval_diagnostics,
            "quality_tier": getattr(canonical_item, "quality_tier", None),
            "reader_eligibility": getattr(canonical_item, "reader_eligibility", None),
            "notebooklm_eligibility": getattr(canonical_item, "notebooklm_eligibility", None),
        }
        quality = evaluate_quality(candidate, policy)
        candidate.update(quality)
        legacy_video["body_kind"] = canonical_item.body_kind
        legacy_video["quality_tier"] = quality["quality_tier"]
        if export_format.startswith("notebooklm"):
            include, reason = should_include_in_notebooklm(candidate, policy)
            if not include:
                if compare:
                    _increment(items_priority_exclusion_reasons, _normalize_export_reason(reason or "excluded"))
                continue
            warnings = migration_warnings + ([reason] if reason else [])
        else:
            include, warnings = should_include_in_reader(candidate, policy)
            if not include:
                if compare:
                    for warning in warnings or ["excluded"]:
                        _increment(items_priority_exclusion_reasons, _normalize_export_reason(warning))
                continue
            warnings = migration_warnings + warnings
        export_rows.append(_to_export_item_from_legacy(legacy_video, warnings))
        if compare:
            _increment(items_priority_source_counts, str(canonical_item.source_id or "unknown"))

    if compare:
        for legacy_video in db.list_legacy_videos():
            if source_ids is not None and "youtube.default" not in source_ids:
                continue
            canonical_item, migration_warnings = map_legacy_video_to_item(legacy_video)
            candidate = {
                "item_id": canonical_item.item_id,
                "external_id": canonical_item.external_id,
                "title": canonical_item.title,
                "author": canonical_item.author,
                "published_at": canonical_item.published_at,
                "url": canonical_item.url,
                "raw_text": canonical_item.raw_text,
                "cleaned_text": canonical_item.cleaned_text,
                "body_kind": canonical_item.body_kind,
                "content_status": canonical_item.content_status,
                "content_warning": canonical_item.content_warning,
                "retrieval_diagnostics": canonical_item.retrieval_diagnostics,
                "quality_tier": getattr(canonical_item, "quality_tier", None),
                "reader_eligibility": getattr(canonical_item, "reader_eligibility", None),
                "notebooklm_eligibility": getattr(canonical_item, "notebooklm_eligibility", None),
            }
            quality = evaluate_quality(candidate, policy)
            candidate.update(quality)
            legacy_video["body_kind"] = canonical_item.body_kind
            legacy_video["quality_tier"] = quality["quality_tier"]
            if export_format.startswith("notebooklm"):
                include, reason = should_include_in_notebooklm(candidate, policy)
                if not include:
                    _increment(legacy_exclusion_reasons, _normalize_export_reason(reason or "excluded"))
                    continue
                warnings = migration_warnings + ([reason] if reason else [])
            else:
                include, warnings = should_include_in_reader(candidate, policy)
                if not include:
                    for warning in warnings or ["excluded"]:
                        _increment(legacy_exclusion_reasons, _normalize_export_reason(warning))
                    continue
                warnings = migration_warnings + warnings
            legacy_only_rows.append(_to_export_item_from_legacy(legacy_video, warnings))
            _increment(legacy_fallback_source_counts, str(canonical_item.source_id or "unknown"))

    if export_format == "reader":
        export_path = export_reader_markdown(export_rows, output_dir=output_dir, query=query)
    elif export_format == "reader-json":
        export_path = export_reader_json(export_rows, output_dir=output_dir, query=query)
    elif export_format == "notebooklm-json":
        export_path = export_notebooklm_json(export_rows, output_dir=output_dir, query=query)
    elif export_format == "notebooklm-markdown":
        export_path = export_notebooklm_markdown(export_rows, output_dir=output_dir, query=query)
    else:
        raise ValueError(f"Unsupported export format: {export_format}")

    if not compare:
        return export_path

    current_by_video = {str(row.get("video_id", "")): row for row in export_rows}
    legacy_by_video = {str(row.get("video_id", "")): row for row in legacy_only_rows}
    overlapping_video_ids = set(current_by_video) & set(legacy_by_video)
    changed_video_count = sum(
        1
        for video_id in overlapping_video_ids
        if current_by_video[video_id] != legacy_by_video[video_id]
    )
    return export_path, ExportCompareReport(
        items_priority_count=len(export_rows),
        legacy_fallback_count=len(legacy_only_rows),
        overlapping_video_count=len(overlapping_video_ids),
        items_priority_only_count=len(set(current_by_video) - set(legacy_by_video)),
        legacy_only_count=len(set(legacy_by_video) - set(current_by_video)),
        changed_video_count=changed_video_count,
        items_priority_source_counts=items_priority_source_counts,
        legacy_fallback_source_counts=legacy_fallback_source_counts,
        source_count_diff={
            source_id: items_priority_source_counts.get(source_id, 0) - legacy_fallback_source_counts.get(source_id, 0)
            for source_id in sorted(set(items_priority_source_counts) | set(legacy_fallback_source_counts))
        },
        items_priority_exclusion_reasons=items_priority_exclusion_reasons,
        legacy_exclusion_reasons=legacy_exclusion_reasons,
        exclusion_reason_diff={
            reason: items_priority_exclusion_reasons.get(reason, 0) - legacy_exclusion_reasons.get(reason, 0)
            for reason in sorted(set(items_priority_exclusion_reasons) | set(legacy_exclusion_reasons))
        },
    )
