from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from config import CONFIG_DIR, load_structured_config
from db.database import Database
from db.repository import CanonicalItem, ItemRepository, source_record_from_dict
from evaluation.quality import evaluate_quality
from normalization.canonicalize import CLEANING_VERSION, build_item_id
from pipeline.report_codes import describe_reason


@dataclass(slots=True)
class BackfillSummary:
    scanned: int = 0
    created: int = 0
    updated: int = 0
    skipped_existing: int = 0
    conflicts: int = 0
    warning_count: int = 0
    error_count: int = 0
    action_counts: dict[str, int] = field(default_factory=dict)
    body_kind_counts: dict[str, int] = field(default_factory=dict)
    evidence_strength_counts: dict[str, int] = field(default_factory=dict)
    warning_code_counts: dict[str, int] = field(default_factory=dict)
    conflict_type_counts: dict[str, int] = field(default_factory=dict)
    audit_records: list[dict[str, Any]] = field(default_factory=list)


def _increment(counter: dict[str, int], key: str) -> None:
    normalized = key or "unknown"
    counter[normalized] = counter.get(normalized, 0) + 1


def _normalize_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _has_summary_payload(short_summary: str, detailed_summary: str) -> bool:
    return bool(short_summary.strip() or detailed_summary.strip())


def _has_protected_outputs(repository: ItemRepository, item_id: str) -> bool:
    return bool(
        repository.get_item_summary(item_id)
        or repository.get_item_chunks(item_id)
        or repository.get_item_chunk_summaries(item_id)
    )


def _detect_conflict_types(
    repository: ItemRepository,
    item_id: str,
    legacy_chunks: list[dict[str, Any]],
    legacy_chunk_summaries: dict[str, dict[str, Any]],
    short_summary: str,
    detailed_summary: str,
) -> list[str]:
    conflict_types: list[str] = []
    existing_summary = repository.get_item_summary(item_id)
    if existing_summary and _has_summary_payload(short_summary, detailed_summary):
        if (
            str(existing_summary.get("short_summary", "") or "") != short_summary
            or str(existing_summary.get("detailed_summary", "") or "") != detailed_summary
        ):
            conflict_types.append("migrate.conflict.item_summary_diff")

    existing_chunks = repository.get_item_chunks(item_id)
    if existing_chunks and legacy_chunks:
        existing_texts = [str(chunk.get("text", "") or "") for chunk in existing_chunks]
        legacy_texts = [str(chunk.get("text", "") or "") for chunk in legacy_chunks]
        if existing_texts != legacy_texts:
            conflict_types.append("migrate.conflict.item_chunks_diff")

    existing_chunk_summaries = repository.get_item_chunk_summaries(item_id)
    if existing_chunk_summaries and legacy_chunk_summaries:
        normalized_existing = {
            str(chunk_id): _normalize_json(summary)
            for chunk_id, summary in existing_chunk_summaries.items()
        }
        normalized_legacy = {
            str(chunk_id): _normalize_json(summary)
            for chunk_id, summary in legacy_chunk_summaries.items()
        }
        if normalized_existing != normalized_legacy:
            conflict_types.append("migrate.conflict.item_chunk_summaries_diff")
    return conflict_types


def _append_audit_record(
    summary: BackfillSummary,
    *,
    legacy_video: dict[str, Any],
    action: str,
    item: CanonicalItem | None,
    warning_code: str = "",
    warning_message: str = "",
    conflict_types: list[str] | None = None,
    applied_resources: list[str] | None = None,
) -> None:
    body_kind = item.body_kind if item else ""
    evidence_strength = item.evidence_strength if item else ""
    record = {
        "legacy_video_id": str(legacy_video.get("video_id", "")),
        "item_id": item.item_id if item else "",
        "action": action,
        "body_kind": body_kind,
        "evidence_strength": evidence_strength,
        "warning_code": warning_code,
        "warning_message": warning_message,
        "conflict_types": list(conflict_types or []),
        "applied_resources": list(applied_resources or []),
    }
    summary.audit_records.append(record)
    _increment(summary.action_counts, action)
    if body_kind:
        _increment(summary.body_kind_counts, body_kind)
    if evidence_strength:
        _increment(summary.evidence_strength_counts, evidence_strength)
    if warning_code:
        _increment(summary.warning_code_counts, warning_code)
    for conflict_type in conflict_types or []:
        _increment(summary.conflict_type_counts, conflict_type)


def write_backfill_reports(
    summary: BackfillSummary,
    *,
    audit_file: str | Path | None = None,
    summary_file: str | Path | None = None,
) -> None:
    if audit_file:
        audit_path = Path(audit_file)
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        audit_path.write_text(
            json.dumps(summary.audit_records, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    if summary_file:
        summary_path = Path(summary_file)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        payload = asdict(summary)
        payload.pop("audit_records", None)
        summary_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def _sync_sources(repository: ItemRepository) -> None:
    config = load_structured_config(CONFIG_DIR / "sources.yaml")
    source_records = [source_record_from_dict(item) for item in config.get("sources", [])]
    repository.sync_sources(source_records)


def map_legacy_video_to_item(video: dict[str, Any]) -> tuple[CanonicalItem, list[str]]:
    warnings: list[str] = []
    raw_text = str(video.get("raw_text", "") or "")
    cleaned_text = str(video.get("cleaned_text", "") or "")
    transcript_source = str(video.get("transcript_source", "") or "").strip().lower()
    content_status = str(video.get("content_status", "") or "").strip().lower() or (
        "available" if cleaned_text.strip() else "unavailable"
    )

    if content_status != "available":
        body_kind = "metadata_only"
        evidence_strength = "none"
    elif transcript_source in {"description", "api_description"}:
        body_kind = "description_only"
        evidence_strength = "weak"
    elif transcript_source in {"manual", "auto", "cached"} and cleaned_text.strip():
        body_kind = "full_text"
        evidence_strength = "medium"
    elif cleaned_text.strip():
        body_kind = "partial_text"
        evidence_strength = "weak"
        warnings.append("unknown_transcript_source_mapped_to_partial_text")
    else:
        body_kind = "metadata_only"
        evidence_strength = "none"
        warnings.append("missing_cleaned_text_mapped_to_metadata_only")

    if transcript_source in {"description", "api_description"} and not raw_text.strip():
        warnings.append("description_source_without_raw_text")
    if not video.get("url"):
        warnings.append("missing_url")

    item = CanonicalItem(
        item_id=build_item_id("youtube.default", str(video.get("video_id", "") or ""), str(video.get("url", "") or "")),
        source_id="youtube.default",
        source_type="youtube_video",
        external_id=str(video.get("video_id", "") or "") or None,
        title=video.get("title"),
        author=video.get("channel"),
        published_at=video.get("published_at"),
        url=str(video.get("url", "") or ""),
        raw_text=raw_text,
        cleaned_text=cleaned_text,
        body_kind=body_kind,
        content_status=content_status,
        content_warning=video.get("content_warning") or None,
        retrieval_diagnostics=dict(video.get("retrieval_diagnostics", {}) or {}),
        language="en",
        trust_level="medium",
        evidence_strength=evidence_strength,
        cleaning_version=CLEANING_VERSION,
        cleaning_diagnostics={
            "migrated_from_legacy": True,
            "legacy_transcript_source": transcript_source,
            "legacy_metadata_only_reason": str(video.get("metadata_only_reason", "") or ""),
            "raw_length": len(raw_text),
            "cleaned_length": len(cleaned_text),
            "warnings": warnings,
        },
    )
    return item, warnings


def _copy_legacy_chunks(
    repository: ItemRepository,
    item_id: str,
    chunks: list[dict[str, Any]],
    chunk_summaries: dict[str, dict[str, Any]],
    *,
    include_summaries: bool = True,
) -> None:
    if not chunks:
        return
    stored_chunks = repository.replace_chunks(item_id, [str(chunk.get("text", "") or "") for chunk in chunks])
    if not include_summaries:
        return
    for stored_chunk, legacy_chunk in zip(stored_chunks, chunks):
        legacy_summary = chunk_summaries.get(str(legacy_chunk.get("chunk_id", "")), {})
        if legacy_summary:
            repository.upsert_chunk_summary(stored_chunk["chunk_id"], legacy_summary)


def _copy_legacy_chunk_summaries_to_existing_chunks(
    repository: ItemRepository,
    item_id: str,
    chunks: list[dict[str, Any]],
    chunk_summaries: dict[str, dict[str, Any]],
) -> None:
    if not chunks or not chunk_summaries:
        return
    existing_chunks = repository.get_item_chunks(item_id)
    for stored_chunk, legacy_chunk in zip(existing_chunks, chunks):
        legacy_summary = chunk_summaries.get(str(legacy_chunk.get("chunk_id", "")), {})
        if legacy_summary:
            repository.upsert_chunk_summary(stored_chunk["chunk_id"], legacy_summary)


def backfill_items_from_videos(
    db: Database,
    *,
    only_missing: bool = False,
    dry_run: bool = False,
) -> BackfillSummary:
    repository = ItemRepository(db)
    _sync_sources(repository)
    policy = load_structured_config(CONFIG_DIR / "policies.yaml")
    summary = BackfillSummary()

    for legacy_video in db.list_legacy_videos():
        summary.scanned += 1
        item: CanonicalItem | None = None
        try:
            item, warnings = map_legacy_video_to_item(legacy_video)
            if warnings:
                summary.warning_count += len(warnings)
            exists = repository.item_exists(item.item_id)
            short_summary = str(legacy_video.get("short_summary", "") or "")
            detailed_summary = str(legacy_video.get("detailed_summary", "") or "")
            legacy_chunks = list(legacy_video.get("chunks", []))
            legacy_chunk_summaries = dict(legacy_video.get("chunk_summaries", {}))
            has_protected_outputs = exists and _has_protected_outputs(repository, item.item_id)
            if exists and only_missing:
                summary.skipped_existing += 1
                _append_audit_record(
                    summary,
                    legacy_video=legacy_video,
                    action="skip",
                    item=item,
                    warning_code="migrate.skip.existing_item_only_missing",
                    warning_message=describe_reason("migrate.skip.existing_item_only_missing"),
                )
                continue
            quality = evaluate_quality(
                {
                    "body_kind": item.body_kind,
                    "content_status": item.content_status,
                    "retrieval_diagnostics": item.retrieval_diagnostics,
                    "cleaned_text": item.cleaned_text,
                },
                policy,
            )
            item.quality_tier = quality["quality_tier"]
            item.reader_eligibility = quality["reader_eligibility"]
            item.notebooklm_eligibility = quality["notebooklm_eligibility"]

            conflict_types = (
                _detect_conflict_types(
                    repository,
                    item.item_id,
                    legacy_chunks,
                    legacy_chunk_summaries,
                    short_summary,
                    detailed_summary,
                )
                if has_protected_outputs
                else []
            )
            has_conflict = bool(conflict_types)

            if dry_run:
                action = "conflict" if has_conflict else ("update" if exists else "create")
                if action == "conflict":
                    summary.conflicts += 1
                    summary.warning_count += len(conflict_types)
                elif exists:
                    summary.updated += 1
                else:
                    summary.created += 1
                _append_audit_record(
                    summary,
                    legacy_video=legacy_video,
                    action=action,
                    item=item,
                    warning_code=conflict_types[0] if conflict_types else "",
                    warning_message=describe_reason(conflict_types[0]) if conflict_types else "",
                    conflict_types=conflict_types,
                    applied_resources=["item"] if exists or not has_conflict else [],
                )
                continue

            repository.upsert_item(item)
            applied_resources = ["item"]
            if "migrate.conflict.item_chunks_diff" not in conflict_types:
                _copy_legacy_chunks(
                    repository,
                    item.item_id,
                    legacy_chunks,
                    legacy_chunk_summaries,
                    include_summaries="migrate.conflict.item_chunk_summaries_diff" not in conflict_types,
                )
                if legacy_chunks:
                    applied_resources.append("item_chunks")
                if legacy_chunk_summaries and "migrate.conflict.item_chunk_summaries_diff" not in conflict_types:
                    applied_resources.append("item_chunk_summaries")
            elif "migrate.conflict.item_chunk_summaries_diff" not in conflict_types and legacy_chunks:
                _copy_legacy_chunk_summaries_to_existing_chunks(
                    repository,
                    item.item_id,
                    legacy_chunks,
                    legacy_chunk_summaries,
                )
                if legacy_chunk_summaries:
                    applied_resources.append("item_chunk_summaries")

            if (
                "migrate.conflict.item_summary_diff" not in conflict_types
                and (short_summary or detailed_summary)
                and (not exists or not repository.get_item_summary(item.item_id))
            ):
                repository.upsert_item_summary(
                    item.item_id,
                    short_summary or "No summary available.",
                    detailed_summary or "No detailed summary available.",
                    summary_version="legacy-backfill.v1",
                )
                applied_resources.append("item_summary")

            if has_conflict:
                summary.conflicts += 1
                summary.warning_count += len(conflict_types)
                _append_audit_record(
                    summary,
                    legacy_video=legacy_video,
                    action="conflict",
                    item=item,
                    warning_code=conflict_types[0],
                    warning_message=describe_reason(conflict_types[0]),
                    conflict_types=conflict_types,
                    applied_resources=applied_resources,
                )
            elif exists:
                summary.updated += 1
                _append_audit_record(
                    summary,
                    legacy_video=legacy_video,
                    action="update",
                    item=item,
                    applied_resources=applied_resources,
                )
            else:
                summary.created += 1
                _append_audit_record(
                    summary,
                    legacy_video=legacy_video,
                    action="create",
                    item=item,
                    applied_resources=applied_resources,
                )
        except Exception as exc:
            summary.error_count += 1
            _append_audit_record(
                summary,
                legacy_video=legacy_video,
                action="error",
                item=item,
                warning_code=type(exc).__name__,
                warning_message=str(exc),
            )
    return summary
