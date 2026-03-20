import argparse
import json
import sys
import warnings
from pathlib import Path


VENDOR_DIR = Path(__file__).resolve().parent / ".vendor"
if VENDOR_DIR.exists():
    sys.path.insert(0, str(VENDOR_DIR))

warnings.filterwarnings(
    "ignore",
    message="Unable to find acceptable character detection dependency.*",
)

import requests

from config import DB_PATH, YOUTUBE_API_KEY
from db.database import Database
from pipeline.metadata_only_report import (
    build_metadata_only_report,
    classify_metadata_only_row,
    classify_retry_policy,
)
from pipeline.pipeline import build_default_pipeline
from outputs.export_notebooklm import export_notebooklm_json, export_notebooklm_markdown
from outputs.export_reader import export_reader_json, export_reader_markdown


def _configure_stdio() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="backslashreplace")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "AI News Intelligence Pipeline MVP "
            "(videos without retrievable content are retained as metadata-only records)"
        )
    )
    parser.add_argument("--query", help="YouTube search query")
    parser.add_argument("--channel-id", help="YouTube channel ID")
    parser.add_argument(
        "--max-videos",
        type=int,
        default=1,
        help="Maximum number of videos to process in this run",
    )
    parser.add_argument(
        "--video-workers",
        type=int,
        default=2,
        help="Number of videos to process concurrently",
    )
    parser.add_argument(
        "--chunk-workers",
        type=int,
        default=2,
        help="Number of chunks to summarize concurrently per video",
    )
    parser.add_argument(
        "--skip-llm",
        action="store_true",
        help="Skip chunk summarization and video summary generation",
    )
    parser.add_argument(
        "--resume-only-missing",
        action="store_true",
        help="Reuse existing transcripts, chunks, and summaries when present",
    )
    parser.add_argument(
        "--skip-existing-videos",
        action="store_true",
        help="Skip videos that already exist in the database",
    )
    parser.add_argument(
        "--export-json",
        action="store_true",
        help="Export run results as JSON into the outputs directory",
    )
    parser.add_argument(
        "--export-markdown",
        action="store_true",
        help="Export run results as Markdown into the outputs directory",
    )
    parser.add_argument(
        "--export-reader-json",
        action="store_true",
        help="Export a reader-friendly JSON digest, including metadata-only unavailable videos",
    )
    parser.add_argument(
        "--export-reader-markdown",
        action="store_true",
        help="Export a reader-friendly Markdown digest, including metadata-only unavailable videos",
    )
    parser.add_argument(
        "--export-notebooklm-json",
        action="store_true",
        help="Export a NotebookLM-ready JSON knowledge pack, including metadata-only unavailable videos",
    )
    parser.add_argument(
        "--export-notebooklm-markdown",
        action="store_true",
        help="Export a NotebookLM-ready Markdown knowledge pack, including metadata-only unavailable videos",
    )
    parser.add_argument(
        "--report-metadata-only",
        action="store_true",
        help="Inspect existing SQLite records whose cleaned content is empty and summarize their patterns",
    )
    parser.add_argument(
        "--retry-metadata-only",
        action="store_true",
        help="Reprocess retryable metadata-only videos already stored in SQLite",
    )
    return parser.parse_args()


def _print_metadata_only_report() -> int:
    db = Database(db_path=DB_PATH, schema_path="db/schema.sql")
    report = build_metadata_only_report(db.get_metadata_only_rows())

    print("Metadata-only report")
    print(f"Total records: {report['total']}")
    if not report["rows"]:
        return 0

    print("Reason counts:")
    for reason, count in sorted(
        report["counts"].items(),
        key=lambda item: (-item[1], item[0]),
    ):
        print(f"- {reason}: {count}")

    if report["retry_policy_counts"]:
        print("Retry policy counts:")
        for policy, count in sorted(
            report["retry_policy_counts"].items(),
            key=lambda item: (-item[1], item[0]),
        ):
            print(f"- {policy}: {count}")

    if report["source_counts"]:
        print("Transcript source counts:")
        for source, count in sorted(
            report["source_counts"].items(),
            key=lambda item: (-item[1], item[0]),
        ):
            print(f"- {source}: {count}")

    print("Diagnostic step counts:")
    for key, counts in report["diagnostics_counts"].items():
        if not counts:
            continue
        formatted = ", ".join(
            f"{value}={count}"
            for value, count in sorted(
                counts.items(),
                key=lambda item: (-item[1], item[0]),
            )
        )
        print(f"- {key}: {formatted}")

    print("Examples by reason:")
    for reason, examples in sorted(
        report["reason_examples"].items(),
        key=lambda item: (-report["counts"].get(item[0], 0), item[0]),
    ):
        print(f"- {reason}:")
        for row in examples:
            title = json.dumps(row["title"], ensure_ascii=False)
            print(
                f"  {row['video_id']} | source={row['transcript_source']} "
                f"| published_at={row['published_at']} | title={title}"
            )

    print("Examples by retry policy:")
    for policy, examples in sorted(
        report["retry_policy_examples"].items(),
        key=lambda item: (-report["retry_policy_counts"].get(item[0], 0), item[0]),
    ):
        print(f"- {policy}:")
        for row in examples:
            title = json.dumps(row["title"], ensure_ascii=False)
            print(
                f"  {row['video_id']} | reason={row['reason']} | source={row['transcript_source']} "
                f"| published_at={row['published_at']} | title={title}"
            )

    print("All rows:")
    for row in report["rows"]:
        title = json.dumps(row["title"], ensure_ascii=False)
        failure_reason = row["retrieval_diagnostics"].get("failure_reason", "")
        print(
            f"- {row['video_id']} | reason={row['reason']} | source={row['transcript_source']} "
            f"| retry_policy={row['retry_policy']} | failure_reason={failure_reason} "
            f"| description_len={row['description_length']} | raw_len={row['raw_text_length']} "
            f"| title={title}"
        )
    return 0


def _select_retryable_metadata_only_videos(
    rows: list[dict[str, object]],
    limit: int | None = None,
) -> list[dict[str, str]]:
    retryable_rows = [
        row
        for row in rows
        if classify_retry_policy(classify_metadata_only_row(row)) == "retryable"
    ]
    if limit is not None:
        retryable_rows = retryable_rows[: max(1, limit)]
    return [
        {
            "video_id": str(row.get("video_id", "")),
            "title": str(row.get("title", "")),
            "channel": str(row.get("channel", "")),
            "published_at": str(row.get("published_at", "")),
            "url": str(row.get("url", "")),
            "description": str(row.get("description", "")),
        }
        for row in retryable_rows
        if row.get("video_id")
    ]


def _build_retry_metadata_only_summary(
    results: list[dict[str, object]],
) -> dict[str, int]:
    total = len(results)
    recovered = sum(
        1 for item in results if str(item.get("content_status", "")) == "available"
    )
    still_unavailable = sum(
        1 for item in results if str(item.get("content_status", "")) == "unavailable"
    )
    other = total - recovered - still_unavailable
    return {
        "total": total,
        "recovered": recovered,
        "still_unavailable": still_unavailable,
        "other": max(0, other),
    }


def main() -> int:
    _configure_stdio()
    args = parse_args()
    if args.report_metadata_only:
        return _print_metadata_only_report()
    if args.retry_metadata_only and args.skip_existing_videos:
        print(
            "--retry-metadata-only cannot be combined with --skip-existing-videos.",
            file=sys.stderr,
        )
        return 1
    if args.retry_metadata_only and (args.query or args.channel_id):
        print(
            "--retry-metadata-only cannot be combined with --query or --channel-id.",
            file=sys.stderr,
        )
        return 1
    if not args.retry_metadata_only and bool(args.query) == bool(args.channel_id):
        print("Provide exactly one of --query or --channel-id.", file=sys.stderr)
        return 1
    if not args.retry_metadata_only and not YOUTUBE_API_KEY:
        print("Set YOUTUBE_API_KEY before running the pipeline.", file=sys.stderr)
        return 1

    pipeline = build_default_pipeline(
        video_workers=args.video_workers,
        chunk_workers=args.chunk_workers,
        skip_llm=args.skip_llm,
        resume_only_missing=args.resume_only_missing,
        skip_existing_videos=args.skip_existing_videos,
    )
    try:
        if args.retry_metadata_only:
            db = Database(db_path=DB_PATH, schema_path="db/schema.sql")
            retryable_videos = _select_retryable_metadata_only_videos(
                db.get_metadata_only_rows(),
                limit=args.max_videos,
            )
            if not retryable_videos:
                print("No retryable metadata-only videos found.")
                return 0
            print(f"Retrying {len(retryable_videos)} metadata-only video(s).")
            results = pipeline.run_with_videos(
                retryable_videos,
                apply_skip_existing=False,
            )
        else:
            results = pipeline.run(
                query=args.query,
                channel_id=args.channel_id,
                limit=args.max_videos,
            )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except requests.RequestException as exc:
        print(f"YouTube pipeline request failed: {exc}", file=sys.stderr)
        return 1

    for item in results:
        print("=" * 80)
        print(item["title"])
        print(item["url"])
        print(f"Channel: {item['channel']}")
        print(f"Transcript source: {item['transcript_source']}")
        print(f"Content status: {item['content_status']}")
        if item.get("metadata_only_reason"):
            print(f"Metadata-only reason: {item['metadata_only_reason']}")
        if item.get("content_warning"):
            print(f"Content warning: {item['content_warning']}")
        print(f"Short summary: {item['short_summary']}")
        print("Detailed summary:")
        print(item["detailed_summary"])

    if results:
        unavailable_count = sum(
            1 for item in results if item.get("content_status") == "unavailable"
        )
        print("=" * 80)
        print(f"Processed {len(results)} video(s).")
        print(
            "Available content: "
            f"{len(results) - unavailable_count} | Metadata-only unavailable: {unavailable_count}"
        )
        if args.retry_metadata_only:
            retry_summary = _build_retry_metadata_only_summary(results)
            print("Retry metadata-only summary:")
            print(f"- Targeted: {retry_summary['total']}")
            print(f"- Recovered: {retry_summary['recovered']}")
            print(f"- Still unavailable: {retry_summary['still_unavailable']}")
            if retry_summary["other"]:
                print(f"- Other status: {retry_summary['other']}")
    else:
        print("No videos processed.")

    output_dir = Path(__file__).resolve().parent / "outputs"
    if args.export_json:
        json_path = export_reader_json(results, output_dir=output_dir / "reader", query=args.query)
        print(f"JSON export: {json_path}")
    if args.export_markdown:
        markdown_path = export_reader_markdown(
            results,
            output_dir=output_dir / "reader",
            query=args.query,
        )
        print(f"Markdown export: {markdown_path}")
    if args.export_reader_json:
        reader_json_path = export_reader_json(
            results,
            output_dir=output_dir / "reader",
            query=args.query,
        )
        print(f"Reader JSON export: {reader_json_path}")
    if args.export_reader_markdown:
        reader_markdown_path = export_reader_markdown(
            results,
            output_dir=output_dir / "reader",
            query=args.query,
        )
        print(f"Reader Markdown export: {reader_markdown_path}")
    if args.export_notebooklm_json:
        notebooklm_json_path = export_notebooklm_json(
            results,
            output_dir=output_dir / "notebooklm",
            query=args.query,
        )
        print(f"NotebookLM JSON export: {notebooklm_json_path}")
    if args.export_notebooklm_markdown:
        notebooklm_markdown_path = export_notebooklm_markdown(
            results,
            output_dir=output_dir / "notebooklm",
            query=args.query,
        )
        print(f"NotebookLM Markdown export: {notebooklm_markdown_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
