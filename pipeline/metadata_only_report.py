from collections import Counter
from typing import Any


REASON_DESCRIPTIONS = {
    "no_retrievable_content": "watch fallback and API description both produced no text",
    "description_cleaned_empty": "description fallback existed but cleaning removed all usable content",
    "caption_cleaned_empty": "caption text existed but cleaning removed all usable content",
    "unknown_empty_content": "empty cleaned text with an unclassified source pattern",
}


def classify_metadata_only_row(row: dict[str, Any]) -> str:
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


def build_metadata_only_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    classified_rows: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()

    for row in rows:
        reason = classify_metadata_only_row(row)
        counts[reason] += 1
        classified_rows.append(
            {
                "video_id": row.get("video_id", ""),
                "title": row.get("title", ""),
                "channel": row.get("channel", ""),
                "published_at": row.get("published_at", ""),
                "url": row.get("url", ""),
                "transcript_source": row.get("transcript_source", ""),
                "transcript_length": int(row.get("transcript_length", 0) or 0),
                "description_length": len((row.get("description") or "").strip()),
                "raw_text_length": len((row.get("raw_text") or "").strip()),
                "reason": reason,
                "reason_description": REASON_DESCRIPTIONS[reason],
            }
        )

    return {
        "total": len(classified_rows),
        "counts": dict(counts),
        "rows": classified_rows,
    }
