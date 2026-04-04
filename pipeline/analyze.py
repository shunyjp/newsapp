from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from config import CONFIG_DIR, load_structured_config
from db.database import Database
from db.repository import CanonicalItem, ItemRepository
from evaluation.quality import evaluate_quality
from llm.summarizer import DEFAULT_SUMMARY, summarize_chunk
from pipeline.report_codes import describe_reason
from pipeline.retry_policy import evaluate_retry_rule, load_retry_policy, normalize_retry_policy
from processing.chunker import split_into_chunks


CONTENT_UNAVAILABLE_SUMMARY = "Content unavailable: transcript and description could not be retrieved."
CONTENT_UNAVAILABLE_DETAIL = (
    "This item was stored with metadata only because analyzable content was not available."
)


@dataclass(slots=True)
class ItemAnalysisState:
    item_id: str
    missing_contents: bool
    missing_cleaned_text: bool
    missing_chunks: bool
    missing_summary: bool
    missing_quality: bool
    is_ineligible: bool
    is_low_quality: bool


@dataclass(slots=True)
class ItemSelectionReasons:
    missing_reasons: list[dict[str, str]]
    retry_reasons: list[dict[str, str]]
    retry_policy_audit: list[dict[str, Any]]


QUALITY_TIER_RANK = {
    "reject": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
}


def _reason_entry(code: str) -> dict[str, str]:
    return {"code": code, "message": describe_reason(code)}


def _retry_requested(reason_code: str, *, retry_ineligible: bool, retry_low_quality: bool) -> bool:
    if reason_code == "analyze.retry.ineligible":
        return retry_ineligible
    if reason_code == "analyze.retry.low_quality":
        return retry_low_quality
    return False


def _quality_rank(value: str | None) -> int:
    return QUALITY_TIER_RANK.get(str(value or "").lower(), -1)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _record_retry_history(
    item: dict[str, Any],
    selected_retry_audit: list[dict[str, Any]],
    *,
    previous_quality_tier: str | None,
    current_quality_tier: str | None,
    attempted_at: str,
    history_limit: int,
) -> None:
    if not selected_retry_audit:
        return
    diagnostics = dict(item.get("cleaning_diagnostics", {}) or {})
    history = dict(diagnostics.get("retry_policy_history", {}) or {})
    quality_improved = _quality_rank(current_quality_tier) > _quality_rank(previous_quality_tier)
    for entry in selected_retry_audit:
        reason_code = str(entry["reason_code"])
        reason_history = list(history.get(reason_code, []) or [])
        reason_history.append(
            {
                "attempted_at": attempted_at,
                "previous_quality_tier": previous_quality_tier,
                "current_quality_tier": current_quality_tier,
                "quality_improved": quality_improved,
            }
        )
        history[reason_code] = reason_history[-max(1, history_limit) :]
    diagnostics["retry_policy_history"] = history
    item["cleaning_diagnostics"] = diagnostics


def _aggregate_video_summary(chunk_summaries: list[dict[str, Any]]) -> dict[str, str]:
    key_points: list[str] = []
    seen: set[str] = set()
    for summary in sorted(
        chunk_summaries,
        key=lambda item: item.get("signal_score", 0.0),
        reverse=True,
    ):
        for point in summary.get("key_points", []):
            normalized = point.strip().lower()
            if normalized and normalized not in seen:
                seen.add(normalized)
                key_points.append(point.strip())
    if not key_points:
        for summary in chunk_summaries:
            fallback = str(summary.get("summary", "")).strip()
            if fallback:
                key_points.append(fallback)
                break
    short_summary = " | ".join(key_points[:3]) if key_points else "No summary available."
    detailed_summary = (
        "\n".join(f"- {point}" for point in key_points) or "No detailed summary available."
    )
    return {
        "short_summary": short_summary,
        "detailed_summary": detailed_summary,
    }


def repository_item_from_row(row: dict[str, Any]) -> CanonicalItem:
    return CanonicalItem(
        item_id=str(row["item_id"]),
        source_id=str(row["source_id"]),
        source_type=str(row["source_type"]),
        external_id=row.get("external_id"),
        title=row.get("title"),
        author=row.get("author"),
        published_at=row.get("published_at"),
        url=str(row["url"]),
        raw_text=str(row.get("raw_text", "") or ""),
        cleaned_text=str(row.get("cleaned_text", "") or ""),
        body_kind=str(row.get("body_kind", "metadata_only")),
        content_status=str(row.get("content_status", "unavailable")),
        content_warning=row.get("content_warning"),
        retrieval_diagnostics=dict(row.get("retrieval_diagnostics", {}) or {}),
        language=row.get("language"),
        trust_level=row.get("trust_level"),
        evidence_strength=str(row.get("evidence_strength", "none")),
        quality_tier=row.get("quality_tier"),
        reader_eligibility=row.get("reader_eligibility"),
        notebooklm_eligibility=row.get("notebooklm_eligibility"),
        cleaning_version=row.get("cleaning_version"),
        cleaning_diagnostics=dict(row.get("cleaning_diagnostics", {}) or {}),
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
    )


def inspect_item_state(repository: ItemRepository, item: dict[str, Any]) -> ItemAnalysisState:
    chunks = repository.get_item_chunks(item["item_id"])
    summary = repository.get_item_summary(item["item_id"])
    cleaned_text = str(item.get("cleaned_text", "") or "")
    return ItemAnalysisState(
        item_id=str(item["item_id"]),
        missing_contents=item.get("raw_text") is None and item.get("cleaned_text") is None,
        missing_cleaned_text=not cleaned_text.strip(),
        missing_chunks=bool(cleaned_text.strip()) and len(chunks) == 0,
        missing_summary=summary is None,
        missing_quality=not bool(item.get("quality_tier")),
        is_ineligible=(
            str(item.get("reader_eligibility", "")) == "ineligible"
            or str(item.get("notebooklm_eligibility", "")) == "ineligible"
        ),
        is_low_quality=str(item.get("quality_tier", "")) == "low",
    )


def should_analyze_item(
    reasons: ItemSelectionReasons,
    *,
    only_missing: bool,
    retry_ineligible: bool,
    retry_low_quality: bool,
) -> bool:
    retry_codes = {reason["code"] for reason in reasons.retry_reasons}
    if retry_ineligible and "analyze.retry.ineligible" in retry_codes:
        return True
    if retry_low_quality and "analyze.retry.low_quality" in retry_codes:
        return True
    if only_missing:
        return bool(reasons.missing_reasons)
    return True


def explain_item_selection(
    state: ItemAnalysisState,
    item: dict[str, Any],
    retry_policy: dict[str, Any] | None = None,
) -> ItemSelectionReasons:
    missing_reasons: list[dict[str, str]] = []
    retry_reasons: list[dict[str, str]] = []
    retry_policy_audit: list[dict[str, Any]] = []
    if state.missing_contents:
        missing_reasons.append(_reason_entry("analyze.missing.contents_absent"))
    if state.missing_cleaned_text:
        missing_reasons.append(_reason_entry("analyze.missing.cleaned_text_empty"))
    if state.missing_chunks:
        missing_reasons.append(_reason_entry("analyze.missing.chunks_missing"))
    if state.missing_summary:
        missing_reasons.append(_reason_entry("analyze.missing.summary_missing"))
    if state.missing_quality:
        missing_reasons.append(_reason_entry("analyze.missing.quality_missing"))
    if state.is_ineligible:
        ineligible_audit = evaluate_retry_rule("analyze.retry.ineligible", item, retry_policy)
        retry_policy_audit.append(ineligible_audit)
        if ineligible_audit["eligible"]:
            retry_reasons.append(_reason_entry("analyze.retry.ineligible"))
    if state.is_low_quality:
        low_quality_audit = evaluate_retry_rule("analyze.retry.low_quality", item, retry_policy)
        retry_policy_audit.append(low_quality_audit)
        if low_quality_audit["eligible"]:
            retry_reasons.append(_reason_entry("analyze.retry.low_quality"))
    return ItemSelectionReasons(
        missing_reasons=missing_reasons,
        retry_reasons=retry_reasons,
        retry_policy_audit=retry_policy_audit,
    )


def build_analysis_report(
    repository: ItemRepository,
    items: list[dict[str, Any]],
    *,
    source_id: str | None,
    source_ids: set[str] | None = None,
    only_missing: bool,
    retry_ineligible: bool,
    retry_low_quality: bool,
    retry_policy: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    active_retry_policy = load_retry_policy() if retry_policy is None else normalize_retry_policy(retry_policy)
    report_rows: list[dict[str, Any]] = []
    for item in items:
        if source_id and item.get("source_id") != source_id:
            continue
        if source_ids is not None and str(item.get("source_id", "")) not in source_ids:
            continue
        state = inspect_item_state(repository, item)
        reasons = explain_item_selection(state, item, active_retry_policy)
        report_rows.append(
            {
                "item_id": item["item_id"],
                "external_id": item.get("external_id", ""),
                "title": item.get("title", ""),
                "source_id": item.get("source_id", ""),
                "missing_reason_codes": [reason["code"] for reason in reasons.missing_reasons],
                "missing_reasons": reasons.missing_reasons,
                "retry_reason_codes": [reason["code"] for reason in reasons.retry_reasons],
                "retry_reasons": reasons.retry_reasons,
                "retry_policy_audit": reasons.retry_policy_audit,
                "retry_policy_applied": [
                    entry for entry in reasons.retry_policy_audit if entry.get("eligible")
                ],
                "retry_candidates": [
                    entry for entry in reasons.retry_policy_audit if entry.get("matched")
                ],
                "selected": should_analyze_item(
                    reasons,
                    only_missing=only_missing,
                    retry_ineligible=retry_ineligible,
                    retry_low_quality=retry_low_quality,
                ),
                "selected_by_retry": any(
                    _retry_requested(
                        str(entry.get("reason_code", "")),
                        retry_ineligible=retry_ineligible,
                        retry_low_quality=retry_low_quality,
                    )
                    and bool(entry.get("eligible"))
                    for entry in reasons.retry_policy_audit
                ),
                "analysis_state": asdict(state),
            }
        )
    return report_rows


def build_analysis_metrics(
    selection_rows: list[dict[str, Any]],
    analyzed_items: list[dict[str, Any]],
) -> dict[str, Any]:
    total_items = len(selection_rows)
    retry_candidate_rows = [row for row in selection_rows if row.get("retry_candidates")]
    retried_rows = [row for row in selection_rows if row.get("selected_by_retry")]
    retried_item_ids = {str(row.get("item_id", "")) for row in retried_rows}
    executed_retries = [
        item
        for item in analyzed_items
        if str(item.get("item_id", "")) in retried_item_ids
    ]
    successful_retries = [
        item
        for item in executed_retries
        if bool(item.get("analysis_report", {}).get("retry_effect", {}).get("quality_improved"))
    ]
    source_retry_distribution: dict[str, dict[str, int]] = {}
    blocked_reason_counts = {
        "blocked_by_max_retries": 0,
        "blocked_by_cooldown": 0,
        "blocked_by_override": 0,
    }
    for row in selection_rows:
        source_id = str(row.get("source_id", "") or "unknown")
        distribution = source_retry_distribution.setdefault(
            source_id,
            {
                "analyzed_count": 0,
                "retry_candidate_count": 0,
                "retried_count": 0,
            },
        )
        distribution["analyzed_count"] += 1
        retry_candidates = list(row.get("retry_candidates", []) or [])
        if retry_candidates:
            distribution["retry_candidate_count"] += 1
        if row.get("selected_by_retry"):
            distribution["retried_count"] += 1
        for entry in retry_candidates:
            blocked_reason = str(entry.get("blocked_reason") or "")
            if blocked_reason == "max_retries_reached":
                blocked_reason_counts["blocked_by_max_retries"] += 1
            elif blocked_reason == "cooldown_active":
                blocked_reason_counts["blocked_by_cooldown"] += 1
            elif blocked_reason == "override_disabled":
                blocked_reason_counts["blocked_by_override"] += 1
    retry_rate = (len(retried_rows) / total_items) if total_items else 0.0
    retry_success_rate = (
        len(successful_retries) / len(executed_retries) if executed_retries else 0.0
    )
    return {
        "total_items": total_items,
        "retry_candidate_items": len(retry_candidate_rows),
        "retried_items": len(retried_rows),
        "retry_rate": retry_rate,
        "retry_success_count": len(successful_retries),
        "retry_success_rate": retry_success_rate,
        "retry_success_definition": "quality_tier_improved",
        "blocked_reason_counts": blocked_reason_counts,
        "source_retry_distribution": source_retry_distribution,
    }


def analyze_items(
    db: Database,
    source_id: str | None = None,
    source_ids: set[str] | None = None,
    only_missing: bool = False,
    retry_ineligible: bool = False,
    retry_low_quality: bool = False,
    skip_llm: bool = False,
    model: str | None = None,
    retry_policy: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    repository = ItemRepository(db)
    policy = load_structured_config(CONFIG_DIR / "policies.yaml")
    active_retry_policy = load_retry_policy() if retry_policy is None else normalize_retry_policy(retry_policy)
    history_limit = int(active_retry_policy.get("history_limit", 10) or 10)
    items = repository.list_items()
    analyzed: list[dict[str, Any]] = []
    for item in items:
        if source_id and item.get("source_id") != source_id:
            continue
        if source_ids is not None and str(item.get("source_id", "")) not in source_ids:
            continue
        state = inspect_item_state(repository, item)
        reasons = explain_item_selection(state, item, active_retry_policy)
        if not should_analyze_item(
            reasons,
            only_missing=only_missing,
            retry_ineligible=retry_ineligible,
            retry_low_quality=retry_low_quality,
        ):
            continue

        previous_quality_tier = item.get("quality_tier")
        selected_retry_audit = [
            entry
            for entry in reasons.retry_policy_audit
            if entry.get("eligible")
            and _retry_requested(
                str(entry.get("reason_code", "")),
                retry_ineligible=retry_ineligible,
                retry_low_quality=retry_low_quality,
            )
        ]
        quality = evaluate_quality(item, policy)
        item.update(quality)
        attempted_at = _utc_now_iso()
        _record_retry_history(
            item,
            selected_retry_audit,
            previous_quality_tier=(
                None if previous_quality_tier is None else str(previous_quality_tier)
            ),
            current_quality_tier=str(item.get("quality_tier") or ""),
            attempted_at=attempted_at,
            history_limit=history_limit,
        )
        repository.upsert_item(repository_item_from_row(item))
        if item.get("content_status") != "available" or not str(item.get("cleaned_text", "")).strip():
            repository.upsert_item_summary(
                item["item_id"],
                CONTENT_UNAVAILABLE_SUMMARY,
                CONTENT_UNAVAILABLE_DETAIL,
            )
            item["analysis_state"] = asdict(state)
            item["analysis_report"] = {
                "missing_reason_codes": [reason["code"] for reason in reasons.missing_reasons],
                "missing_reasons": reasons.missing_reasons,
                "retry_reason_codes": [reason["code"] for reason in reasons.retry_reasons],
                "retry_reasons": reasons.retry_reasons,
                "retry_policy_applied": selected_retry_audit,
                "retry_policy_audit": reasons.retry_policy_audit,
                "retry_effect": {
                    "previous_quality_tier": previous_quality_tier,
                    "current_quality_tier": item.get("quality_tier"),
                    "quality_improved": _quality_rank(item.get("quality_tier"))
                    > _quality_rank(previous_quality_tier),
                },
            }
            analyzed.append(item)
            continue

        chunks = repository.get_item_chunks(item["item_id"])
        if not chunks or retry_ineligible or retry_low_quality or only_missing:
            chunks = repository.replace_chunks(
                item["item_id"],
                split_into_chunks(item.get("cleaned_text", "")),
            )
        chunk_summaries: list[dict[str, Any]] = []
        existing_summaries = repository.get_item_chunk_summaries(item["item_id"])
        if not skip_llm:
            for chunk in chunks:
                if (
                    not retry_ineligible
                    and not retry_low_quality
                    and not only_missing
                    and chunk["chunk_id"] in existing_summaries
                ):
                    chunk_summaries.append(existing_summaries[chunk["chunk_id"]])
                    continue
                try:
                    summary = (
                        summarize_chunk(chunk["text"], model=model)
                        if model
                        else summarize_chunk(chunk["text"])
                    )
                except Exception:
                    summary = dict(DEFAULT_SUMMARY)
                repository.upsert_chunk_summary(chunk["chunk_id"], summary)
                chunk_summaries.append(summary)
        else:
            chunk_summaries = [existing_summaries[chunk["chunk_id"]] for chunk in chunks if chunk["chunk_id"] in existing_summaries]

        aggregated = _aggregate_video_summary(chunk_summaries)
        repository.upsert_item_summary(
            item["item_id"],
            aggregated["short_summary"],
            aggregated["detailed_summary"],
        )
        item.update(aggregated)
        item["analysis_state"] = asdict(state)
        item["analysis_report"] = {
            "missing_reason_codes": [reason["code"] for reason in reasons.missing_reasons],
            "missing_reasons": reasons.missing_reasons,
            "retry_reason_codes": [reason["code"] for reason in reasons.retry_reasons],
            "retry_reasons": reasons.retry_reasons,
            "retry_policy_applied": selected_retry_audit,
            "retry_policy_audit": reasons.retry_policy_audit,
            "retry_effect": {
                "previous_quality_tier": previous_quality_tier,
                "current_quality_tier": item.get("quality_tier"),
                "quality_improved": _quality_rank(item.get("quality_tier"))
                > _quality_rank(previous_quality_tier),
            },
        }
        analyzed.append(item)
    return analyzed
