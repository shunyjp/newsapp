from collections import Counter
import json
from typing import Any


REASON_DESCRIPTIONS = {
    "watch_page_request_failed": "watch page request failed before transcript or description could be extracted",
    "player_response_unavailable": "watch page loaded but player response could not be parsed",
    "caption_fetch_failed_and_description_empty": "caption track fetch failed and description fallback was empty",
    "no_caption_tracks_and_description_empty": "player response exposed no caption tracks and description fallback was empty",
    "caption_track_empty_and_description_empty": "caption track existed but returned no usable text and description fallback was empty",
    "description_empty_after_caption_fallback": "caption fallback produced no text and extracted description was empty",
    "no_retrievable_content": "watch fallback and API description both produced no text",
    "description_cleaned_empty": "description fallback existed but cleaning removed all usable content",
    "caption_cleaned_empty": "caption text existed but cleaning removed all usable content",
    "unknown_empty_content": "empty cleaned text with an unclassified source pattern",
}

RETRY_POLICY_BY_REASON = {
    "watch_page_request_failed": "retryable",
    "player_response_unavailable": "retryable",
    "caption_fetch_failed_and_description_empty": "retryable",
    "no_caption_tracks_and_description_empty": "non_retryable",
    "caption_track_empty_and_description_empty": "review_needed",
    "description_empty_after_caption_fallback": "non_retryable",
    "no_retrievable_content": "non_retryable",
    "description_cleaned_empty": "review_needed",
    "caption_cleaned_empty": "review_needed",
    "unknown_empty_content": "review_needed",
}

RETRY_POLICY_DESCRIPTIONS = {
    "retryable": "temporary retrieval failure patterns that should be retried later",
    "non_retryable": "content is likely unavailable with the current retrieval strategy",
    "review_needed": "manual review is recommended before deciding whether to retry",
}

DIAGNOSTIC_KEYS = [
    "watch_html",
    "player_response",
    "caption_tracks",
    "selected_caption_source",
    "caption_fetch",
    "description",
]


def _normalize_diagnostics(raw_diagnostics: Any) -> dict[str, Any]:
    diagnostics = raw_diagnostics or {}
    if isinstance(diagnostics, str):
        try:
            diagnostics = json.loads(diagnostics)
        except json.JSONDecodeError:
            diagnostics = {}
    if not isinstance(diagnostics, dict):
        return {}
    return diagnostics


def classify_metadata_only_row(row: dict[str, Any]) -> str:
    metadata_only_reason = (row.get("metadata_only_reason") or "").strip()
    if metadata_only_reason:
        return metadata_only_reason

    transcript_source = (row.get("transcript_source") or "").strip()
    description = (row.get("description") or "").strip()
    raw_text = (row.get("raw_text") or "").strip()

    if transcript_source == "none" and not description and not raw_text:
        return "no_retrievable_content"
    if transcript_source in {"description", "api_description"}:
        return "description_cleaned_empty"
    if transcript_source in {"manual", "auto", "cached"} or raw_text:
        return "caption_cleaned_empty"
    return "unknown_empty_content"


def classify_retry_policy(reason: str) -> str:
    return RETRY_POLICY_BY_REASON.get(reason, "review_needed")


def build_metadata_only_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    classified_rows: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    retry_policy_counts: Counter[str] = Counter()
    source_counts: Counter[str] = Counter()
    warning_counts: Counter[str] = Counter()
    reason_examples: dict[str, list[dict[str, Any]]] = {}
    retry_policy_examples: dict[str, list[dict[str, Any]]] = {}
    diagnostics_counts = {key: Counter[str]() for key in DIAGNOSTIC_KEYS}

    for row in rows:
        reason = classify_metadata_only_row(row)
        retry_policy = classify_retry_policy(reason)
        diagnostics = _normalize_diagnostics(row.get("retrieval_diagnostics"))
        counts[reason] += 1
        retry_policy_counts[retry_policy] += 1
        transcript_source = row.get("transcript_source", "")
        content_warning = row.get("content_warning", "")
        if transcript_source:
            source_counts[transcript_source] += 1
        if content_warning:
            warning_counts[content_warning] += 1
        for key in DIAGNOSTIC_KEYS:
            value = str(diagnostics.get(key, "missing") or "missing")
            diagnostics_counts[key][value] += 1

        example = {
            "video_id": row.get("video_id", ""),
            "title": row.get("title", ""),
            "transcript_source": transcript_source,
            "content_warning": content_warning,
            "published_at": row.get("published_at", ""),
        }
        existing_examples = reason_examples.setdefault(reason, [])
        if len(existing_examples) < 3:
            existing_examples.append(example)
        existing_retry_examples = retry_policy_examples.setdefault(retry_policy, [])
        if len(existing_retry_examples) < 3:
            existing_retry_examples.append(
                {
                    **example,
                    "reason": reason,
                }
            )

        classified_rows.append(
            {
                "video_id": row.get("video_id", ""),
                "title": row.get("title", ""),
                "channel": row.get("channel", ""),
                "published_at": row.get("published_at", ""),
                "url": row.get("url", ""),
                "transcript_source": transcript_source,
                "transcript_length": int(row.get("transcript_length", 0) or 0),
                "content_status": row.get("content_status", ""),
                "content_warning": content_warning,
                "description_length": len((row.get("description") or "").strip()),
                "raw_text_length": len((row.get("raw_text") or "").strip()),
                "reason": reason,
                "reason_description": REASON_DESCRIPTIONS.get(
                    reason,
                    "runtime retrieval diagnostics captured a more specific metadata-only reason",
                ),
                "retry_policy": retry_policy,
                "retry_policy_description": RETRY_POLICY_DESCRIPTIONS[retry_policy],
                "retrieval_diagnostics": diagnostics,
            }
        )

    return {
        "total": len(classified_rows),
        "counts": dict(counts),
        "retry_policy_counts": dict(retry_policy_counts),
        "source_counts": dict(source_counts),
        "warning_counts": dict(warning_counts),
        "diagnostics_counts": {
            key: dict(counter)
            for key, counter in diagnostics_counts.items()
        },
        "reason_examples": reason_examples,
        "retry_policy_examples": retry_policy_examples,
        "rows": classified_rows,
    }
