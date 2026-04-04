import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "notebooklm-pack.v1"


def _is_content_unavailable(item: dict[str, Any]) -> bool:
    return item.get("content_status", "available") == "unavailable"


def _as_string(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    if isinstance(value, str):
        return value
    return str(value)


def _as_int(value: Any, fallback: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _as_float(value: Any, fallback: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _as_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        values: list[str] = []
        for item in value:
            text = _as_string(item).strip()
            if text:
                values.append(text)
        return values
    if isinstance(value, tuple):
        values: list[str] = []
        for item in value:
            text = _as_string(item).strip()
            if text:
                values.append(text)
        return values
    text = _as_string(value).strip()
    return [text] if text else []


def _slugify(value: str) -> str:
    normalized = value.strip().lower()
    normalized = re.sub(r"[^\w\s-]", "", normalized)
    normalized = re.sub(r"[-\s]+", "-", normalized)
    return normalized.strip("-") or "run"


def _build_output_path(
    output_dir: str | Path,
    prefix: str,
    query: str | None,
    extension: str,
) -> Path:
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    query_part = f"-{_slugify(query)}" if query else ""
    return directory / f"{prefix}{query_part}-{timestamp}.{extension}"


def _build_chunk_entry(chunk: dict[str, Any]) -> dict[str, Any]:
    return {
        "chunk_id": _as_string(chunk.get("chunk_id", "")),
        "chunk_no": _as_int(chunk.get("chunk_no", 0)),
        "signal_score": _as_float(chunk.get("signal_score", 0.0)),
        "summary": _as_string(chunk.get("summary", "")),
        "key_points": _as_string_list(chunk.get("key_points", [])),
        "entities": _as_string_list(chunk.get("entities", [])),
        "categories": _as_string_list(chunk.get("category", [])),
        "text": _as_string(chunk.get("text", "")),
    }


def _build_video_document(item: dict[str, Any]) -> dict[str, Any]:
    content_unavailable = _is_content_unavailable(item)
    return {
        "video": {
            "video_id": _as_string(item["video_id"]),
            "item_id": _as_string(item.get("item_id", "")),
            "title": _as_string(item["title"]),
            "channel": _as_string(item.get("channel", "")),
            "published_at": _as_string(item.get("published_at", "")),
            "url": _as_string(item["url"]),
        },
        "retrieval": {
            "transcript_source": _as_string(item.get("transcript_source", "")),
            "transcript_length": _as_int(item.get("transcript_length", 0)),
            "content_status": _as_string(item.get("content_status", "available"), "available"),
            "content_warning": _as_string(item.get("content_warning", "")),
            "body_kind": _as_string(item.get("body_kind", "")),
            "quality_tier": _as_string(item.get("quality_tier", "")),
            "is_metadata_only": content_unavailable,
        },
        "summary": {
            "short_summary": _as_string(
                item.get("short_summary", "No summary available."),
                "No summary available.",
            ),
            "detailed_summary": _as_string(
                item.get("detailed_summary", "No detailed summary available."),
                "No detailed summary available.",
            ),
            "why_it_matters": _as_string(item.get("why_it_matters", "")),
            "signal_score": _as_float(item.get("signal_score", 0.0)),
        },
        "analysis": {
            "reader_points": _as_string_list(item.get("reader_points", [])),
            "aggregated_key_points": _as_string_list(item.get("aggregated_key_points", [])),
            "aggregated_entities": _as_string_list(item.get("aggregated_entities", [])),
            "aggregated_categories": _as_string_list(item.get("aggregated_categories", [])),
        },
        "evidence": {
            "chunk_count": len(item.get("chunk_summaries", [])),
            "chunks": [_build_chunk_entry(chunk) for chunk in item.get("chunk_summaries", [])],
            "cleaned_text": _as_string(item.get("cleaned_text", "")),
        },
    }


def _build_payload(
    results: list[dict[str, Any]],
    query: str | None,
) -> dict[str, Any]:
    documents = [_build_video_document(item) for item in results]
    available_count = sum(1 for item in results if not _is_content_unavailable(item))
    unavailable_count = sum(1 for item in results if _is_content_unavailable(item))
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now().isoformat(),
        "query": query,
        "stats": {
            "video_count": len(results),
            "available_count": available_count,
            "metadata_only_count": unavailable_count,
        },
        "documents": documents,
    }


def export_notebooklm_json(
    results: list[dict[str, Any]],
    output_dir: str | Path,
    query: str | None = None,
) -> Path:
    output_path = _build_output_path(
        output_dir=output_dir,
        prefix="notebooklm-pack",
        query=query,
        extension="json",
    )
    payload = _build_payload(results, query=query)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return output_path


def _append_bullet_list(lines: list[str], values: list[str], fallback: str) -> None:
    if values:
        lines.extend(f"- {value}" for value in values)
    else:
        lines.append(f"- {fallback}")


def _append_chunk_section(lines: list[str], chunks: list[dict[str, Any]], fallback: str) -> None:
    if not chunks:
        lines.extend(["No chunk evidence available.", ""])
        return

    for chunk in chunks:
        lines.extend(
            [
                f"#### Chunk {chunk['chunk_no']}",
                "",
                f"- Chunk ID: {chunk['chunk_id']}",
                f"- Signal score: {chunk['signal_score']:.2f}",
                "",
                "Summary:",
                "",
                chunk["summary"] or fallback,
                "",
                "Key points:",
            ]
        )
        _append_bullet_list(lines, chunk["key_points"], "None")
        lines.extend(["", "Entities:"])
        _append_bullet_list(lines, chunk["entities"], "None")
        lines.extend(["", "Categories:"])
        _append_bullet_list(lines, chunk["categories"], "None")
        lines.extend(["", "Chunk text:", "", chunk["text"] or fallback, ""])


def export_notebooklm_markdown(
    results: list[dict[str, Any]],
    output_dir: str | Path,
    query: str | None = None,
) -> Path:
    output_path = _build_output_path(
        output_dir=output_dir,
        prefix="notebooklm-pack",
        query=query,
        extension="md",
    )
    payload = _build_payload(results, query=query)
    stats = payload["stats"]
    lines = [
        "# NotebookLM Knowledge Pack",
        "",
        "## Pack Metadata",
        "",
        f"- Schema version: {payload['schema_version']}",
        f"- Generated at: {payload['generated_at']}",
        f"- Query: {query or '(none)'}",
        f"- Videos: {stats['video_count']}",
        f"- Available content: {stats['available_count']}",
        f"- Metadata-only unavailable: {stats['metadata_only_count']}",
        "",
    ]

    for index, document in enumerate(payload["documents"], start=1):
        video = document["video"]
        retrieval = document["retrieval"]
        summary = document["summary"]
        analysis = document["analysis"]
        evidence = document["evidence"]
        status_label = " [Metadata Only]" if retrieval["is_metadata_only"] else ""
        retrieval_note = (
            retrieval["content_warning"] or "Content retrieved successfully."
        )
        missing_content_fallback = (
            retrieval["content_warning"] or "No extracted evidence available."
        )

        lines.extend(
            [
                f"## Video {index}: {video['title']}{status_label}",
                "",
                "### Source Metadata",
                "",
                f"- Video ID: {video['video_id']}",
                f"- Channel: {video['channel']}",
                f"- Published: {video['published_at']}",
                f"- URL: {video['url']}",
                "",
                "### Retrieval Status",
                "",
                f"- Transcript source: {retrieval['transcript_source']}",
                f"- Transcript length: {retrieval['transcript_length']}",
                f"- Content status: {retrieval['content_status']}",
                "",
                retrieval_note,
                "",
                "### NotebookLM Summary",
                "",
                f"Short summary: {summary['short_summary']}",
                "",
                "Detailed summary:",
                "",
                summary["detailed_summary"],
                "",
                f"Why it matters: {summary['why_it_matters'] or missing_content_fallback}",
                "",
                f"Signal score: {summary['signal_score']:.2f}",
                "",
                "### Aggregated Evidence",
                "",
                "Key points:",
            ]
        )
        _append_bullet_list(
            lines,
            analysis["aggregated_key_points"],
            missing_content_fallback,
        )
        lines.extend(["", "Entities:"])
        _append_bullet_list(
            lines,
            analysis["aggregated_entities"],
            missing_content_fallback,
        )
        lines.extend(["", "Categories:"])
        _append_bullet_list(
            lines,
            analysis["aggregated_categories"],
            missing_content_fallback,
        )
        lines.extend(["", "### Chunk Evidence", ""])
        _append_chunk_section(
            lines,
            evidence["chunks"],
            missing_content_fallback,
        )
        lines.extend(
            [
                "### Cleaned Transcript",
                "",
                evidence["cleaned_text"] or missing_content_fallback,
                "",
            ]
        )

    output_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    return output_path
