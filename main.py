import argparse
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

from config import YOUTUBE_API_KEY
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
    return parser.parse_args()


def main() -> int:
    _configure_stdio()
    args = parse_args()
    if bool(args.query) == bool(args.channel_id):
        print("Provide exactly one of --query or --channel-id.", file=sys.stderr)
        return 1
    if not YOUTUBE_API_KEY:
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
