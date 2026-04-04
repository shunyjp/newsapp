import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any


def _is_content_unavailable(item: dict[str, Any]) -> bool:
    return item.get("content_status", "available") == "unavailable"


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


def export_reader_json(
    results: list[dict[str, Any]],
    output_dir: str | Path,
    query: str | None = None,
) -> Path:
    output_path = _build_output_path(
        output_dir=output_dir,
        prefix="reader-digest",
        query=query,
        extension="json",
    )
    simplified_results = []
    for item in results:
        simplified_results.append(
            {
                "video_id": item["video_id"],
                "title": item["title"],
                "channel": item.get("channel", ""),
                "published_at": item.get("published_at", ""),
                "url": item["url"],
                "item_id": item.get("item_id", ""),
                "transcript_source": item.get("transcript_source", ""),
                "content_status": item.get("content_status", "available"),
                "content_warning": item.get("content_warning", ""),
                "body_kind": item.get("body_kind", ""),
                "quality_tier": item.get("quality_tier", ""),
                "reader_warning_flags": item.get("reader_warning_flags", []),
                "signal_score": item.get("signal_score", 0.0),
                "short_summary": item.get("short_summary", "No summary available."),
                "reader_points": item.get("reader_points", []),
                "why_it_matters": item.get("why_it_matters", ""),
            }
        )

    payload = {
        "generated_at": datetime.now().isoformat(),
        "query": query,
        "count": len(simplified_results),
        "available_count": sum(
            1 for item in simplified_results if item.get("content_status") == "available"
        ),
        "unavailable_count": sum(
            1 for item in simplified_results if item.get("content_status") == "unavailable"
        ),
        "results": simplified_results,
    }
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return output_path


def export_reader_markdown(
    results: list[dict[str, Any]],
    output_dir: str | Path,
    query: str | None = None,
) -> Path:
    output_path = _build_output_path(
        output_dir=output_dir,
        prefix="reader-digest",
        query=query,
        extension="md",
    )
    lines = [
        "# AI News Reader Digest",
        "",
        f"- Generated at: {datetime.now().isoformat()}",
        f"- Query: {query or '(none)'}",
        f"- Videos: {len(results)}",
        f"- Available content: {sum(1 for item in results if not _is_content_unavailable(item))}",
        f"- Metadata-only unavailable: {sum(1 for item in results if _is_content_unavailable(item))}",
        "",
    ]

    for item in results:
        status_label = " [Metadata Only]" if _is_content_unavailable(item) else ""
        lines.extend(
            [
                f"## {item['title']}{status_label}",
                "",
                f"- Channel: {item.get('channel', '')}",
                f"- Published: {item.get('published_at', '')}",
                f"- URL: {item['url']}",
                f"- Transcript source: {item.get('transcript_source', '')}",
                f"- Content status: {item.get('content_status', 'available')}",
                f"- Body kind: {item.get('body_kind', '')}",
                f"- Quality tier: {item.get('quality_tier', '')}",
                f"- Signal score: {item.get('signal_score', 0.0):.2f}",
                "",
                "### Retrieval Note",
                "",
                item.get("content_warning") or "Content retrieved successfully.",
                "",
                "### Reader Warnings",
                "",
            ]
        )
        warning_flags = item.get("reader_warning_flags", [])
        if warning_flags:
            lines.extend([f"- {warning}" for warning in warning_flags])
        else:
            lines.append("- none")
        lines.extend(
            [
                "",
                "### Quick Summary",
                "",
                item.get("short_summary", "No summary available."),
                "",
                "### Key Points",
                "",
            ]
        )
        points = item.get("reader_points", [])
        if points:
            lines.extend([f"- {point}" for point in points])
        else:
            fallback = item.get("content_warning") or "No key points available."
            lines.append(f"- {fallback}")
        lines.extend(
            [
                "",
                "### Why It Matters",
                "",
                item.get("content_warning")
                or item.get("why_it_matters", "No context available."),
                "",
            ]
        )

    output_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    return output_path
