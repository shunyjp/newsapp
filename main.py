import argparse
import json
import sys
import warnings
from dataclasses import asdict
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
from pipeline.collect import collect_items
from db.repository import ItemRepository
from pipeline.cleanup import cleanup_explicit_noise_items
from pipeline.analyze import analyze_items, build_analysis_metrics, build_analysis_report
from pipeline.export import export_items
from pipeline.migrate import backfill_items_from_videos, write_backfill_reports
from pipeline.pipeline import build_default_pipeline
from pipeline.reporting import build_run_label, copy_report_artifact, copy_to_latest, write_report_json
from pipeline.source_config import load_source_config, resolve_source_ids
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
    subparsers = parser.add_subparsers(dest="command")

    collect_parser = subparsers.add_parser("collect", help="Collect items from a source provider")
    collect_parser.add_argument("--source", help="Registered source id")
    collect_parser.add_argument("--source-set", help="Configured source set name")
    collect_parser.add_argument("--query", help="Source query")
    collect_parser.add_argument("--channel-id", help="YouTube channel ID")
    collect_parser.add_argument("--max-items", type=int, default=5, help="Maximum items to collect")

    analyze_parser = subparsers.add_parser("analyze", help="Analyze collected items")
    analyze_parser.add_argument("--source", help="Registered source id to analyze")
    analyze_parser.add_argument("--source-set", help="Configured source set name to analyze")
    analyze_parser.add_argument("--item-source", help="Alias of --source for item-based analyze selection")
    analyze_parser.add_argument("--only-missing", action="store_true", help="Analyze only items missing quality/summary data")
    analyze_parser.add_argument("--retry-ineligible", action="store_true", help="Retry items currently marked ineligible")
    analyze_parser.add_argument("--retry-low-quality", action="store_true", help="Retry items currently marked low quality")
    analyze_parser.add_argument("--skip-llm", action="store_true", help="Skip chunk summarization")
    analyze_parser.add_argument("--report-file", help="Write item-level missing/retry reason report as JSON")
    analyze_parser.add_argument("--explain", action="store_true", help="Print item-level missing/retry reasons")

    export_parser = subparsers.add_parser("export", help="Export analyzed items")
    export_parser.add_argument("--source", help="Registered source id to export")
    export_parser.add_argument("--source-set", help="Configured source set name to export")
    export_parser.add_argument(
        "--format",
        required=True,
        choices=("reader", "reader-json", "notebooklm-json", "notebooklm-markdown"),
        help="Export format",
    )
    export_parser.add_argument("--query", help="Optional query label for output file naming")
    export_parser.add_argument("--compare", action="store_true", help="Compare items-priority export against legacy fallback results")

    migrate_parser = subparsers.add_parser("migrate", help="Backfill new item tables from legacy video tables")
    migrate_parser.add_argument("--backfill-items-from-videos", action="store_true", help="Backfill items/item_contents/item_chunks/item_summaries from legacy video tables")
    migrate_parser.add_argument("--only-missing", action="store_true", help="Only create missing item rows")
    migrate_parser.add_argument("--dry-run", action="store_true", help="Estimate migration counts without writing")
    migrate_parser.add_argument("--audit-file", help="Write per-legacy-record migration audit JSON")
    migrate_parser.add_argument("--summary-file", help="Write migration summary JSON")

    cleanup_parser = subparsers.add_parser("cleanup", help="Remove explicit noise items from item tables")
    cleanup_parser.add_argument("--source", help="Registered source id to clean")
    cleanup_parser.add_argument("--source-set", help="Configured source set name to clean")
    cleanup_parser.add_argument("--remove-explicit-noise", action="store_true", help="Remove items whose titles match explicit PR/advertorial patterns")
    cleanup_parser.add_argument("--dry-run", action="store_true", help="List cleanup targets without deleting them")

    parser.add_argument(
        "--reports-root",
        default=str(Path(__file__).resolve().parent / "reports"),
        help="Directory for dated operational report outputs",
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


def _run_collect_command(args: argparse.Namespace) -> int:
    db = Database(db_path=DB_PATH, schema_path="db/schema.sql")
    config = load_source_config()
    source_ids = resolve_source_ids(config, source_id=args.source, source_set=args.source_set)
    all_records: list[dict[str, object]] = []
    source_errors: list[dict[str, str]] = []
    for source_id in source_ids:
        try:
            records = collect_items(
                db=db,
                source_id=source_id,
                source_set=None,
                query=args.query,
                channel_id=args.channel_id,
                max_items=args.max_items,
            )
            all_records.extend(records)
        except Exception as exc:
            source_errors.append({"source_id": source_id, "error": str(exc)})
            print(f"Collect failed for source={source_id}: {exc}", file=sys.stderr)
    print(f"Collected {len(all_records)} item(s) from {args.source_set or args.source}.")
    if source_errors:
        print(f"Source failures: {len(source_errors)}")
    reports_root = Path(args.reports_root)
    collect_report_path = write_report_json(
        reports_root,
        "collect",
        f"collect-result-{build_run_label(args.source, args.source_set)}",
        {
            "source": args.source,
            "source_set": args.source_set,
            "query": args.query,
            "channel_id": args.channel_id,
            "max_items": args.max_items,
            "record_count": len(all_records),
            "error_count": len(source_errors),
            "errors": source_errors,
            "records": [
                {
                    **{key: value for key, value in record.items() if key != "item"},
                    "item": asdict(record["item"]),
                }
                for record in all_records
            ],
        },
    )
    latest_collect_report_path = copy_to_latest(reports_root, "collect", collect_report_path)
    for record in all_records:
        item = record["item"]
        print("=" * 80)
        print(item.title or "(untitled)")
        print(item.url)
        print(f"Body kind: {item.body_kind}")
        print(f"Content status: {item.content_status}")
    print(f"Collect report written: {collect_report_path}")
    print(f"Latest collect report: {latest_collect_report_path}")
    return 0


def _run_analyze_command(args: argparse.Namespace) -> int:
    db = Database(db_path=DB_PATH, schema_path="db/schema.sql")
    source_id = args.item_source or args.source
    source_ids = None
    single_source_id = source_id
    if source_id or args.source_set:
        config = load_source_config()
        source_ids = set(
            resolve_source_ids(config, source_id=source_id, source_set=args.source_set)
        )
        single_source_id = next(iter(source_ids)) if len(source_ids) == 1 else None
    repository = ItemRepository(db)
    selection_rows = build_analysis_report(
        repository,
        repository.list_items(),
        source_id=single_source_id,
        source_ids=source_ids,
        only_missing=args.only_missing,
        retry_ineligible=args.retry_ineligible,
        retry_low_quality=args.retry_low_quality,
    )
    if args.explain:
        if args.explain:
            print("Analysis selection report")
            for row in selection_rows:
                print("=" * 80)
                print(row.get("title", "(untitled)"))
                print(row.get("item_id", ""))
                print(f"Selected: {row.get('selected', False)}")
                missing_codes = ", ".join(row.get("missing_reason_codes", [])) or "-"
                retry_codes = ", ".join(row.get("retry_reason_codes", [])) or "-"
                missing_messages = ", ".join(reason["message"] for reason in row.get("missing_reasons", [])) or "-"
                retry_messages = ", ".join(reason["message"] for reason in row.get("retry_reasons", [])) or "-"
                print(f"Missing reason codes: {missing_codes}")
                print(f"Missing reasons: {missing_messages}")
                print(f"Retry reason codes: {retry_codes}")
                print(f"Retry reasons: {retry_messages}")
                if row.get("retry_policy_audit"):
                    print("Retry policy audit: " + json.dumps(row["retry_policy_audit"], ensure_ascii=False, sort_keys=True))
    analyzed = analyze_items(
        db=db,
        source_id=single_source_id,
        source_ids=source_ids,
        only_missing=args.only_missing,
        retry_ineligible=args.retry_ineligible,
        retry_low_quality=args.retry_low_quality,
        skip_llm=args.skip_llm,
    )
    analysis_metrics = build_analysis_metrics(selection_rows, analyzed)
    report_payload = {
        "source": source_id,
        "source_set": args.source_set,
        "selection_rows": selection_rows,
        "analyzed_items": analyzed,
        "retry_metrics": analysis_metrics,
        "readme": {
            "retry_success_definition": "A retry counts as success only when quality_tier improved relative to the previous attempt.",
            "blocked_reason_counts": "Counts retry candidates blocked by retry policy gates such as max retries, cooldown, or override-disabled rules.",
            "source_retry_distribution": "Per source summary of analyzed items, retry candidates, and executed retries.",
        },
    }
    if args.report_file:
        report_path = Path(args.report_file)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(report_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"Analysis report written: {report_path}")
    reports_root = Path(args.reports_root)
    auto_report_path = write_report_json(
        reports_root,
        "analyze",
        f"analyze-report-{build_run_label(source_id, args.source_set)}",
        report_payload,
    )
    latest_analyze_report_path = copy_to_latest(reports_root, "analyze", auto_report_path)
    print(f"Analyzed {len(analyzed)} item(s).")
    print("Retry metrics:")
    print(f"- retry_rate: {analysis_metrics['retry_rate']:.3f}")
    print(f"- retry_success_rate: {analysis_metrics['retry_success_rate']:.3f}")
    print(f"- retry_success_definition: {analysis_metrics['retry_success_definition']}")
    print("- blocked_reason_counts: " + json.dumps(analysis_metrics["blocked_reason_counts"], ensure_ascii=False, sort_keys=True))
    print("- source_retry_distribution: " + json.dumps(analysis_metrics["source_retry_distribution"], ensure_ascii=False, sort_keys=True))
    for item in analyzed:
        print("=" * 80)
        print(item.get("title", "(untitled)"))
        print(item.get("url", ""))
        print(f"Quality: {item.get('quality_tier', '')}")
        print(f"Reader eligibility: {item.get('reader_eligibility', '')}")
        print(f"NotebookLM eligibility: {item.get('notebooklm_eligibility', '')}")
        if item.get("analysis_report"):
            print("Missing reason codes: " + (", ".join(item["analysis_report"].get("missing_reason_codes", [])) or "-"))
            print("Missing reasons: " + (", ".join(reason["message"] for reason in item["analysis_report"].get("missing_reasons", [])) or "-"))
            print("Retry reason codes: " + (", ".join(item["analysis_report"].get("retry_reason_codes", [])) or "-"))
            print("Retry reasons: " + (", ".join(reason["message"] for reason in item["analysis_report"].get("retry_reasons", [])) or "-"))
            print("Retry policy applied: " + json.dumps(item["analysis_report"].get("retry_policy_applied", []), ensure_ascii=False, sort_keys=True))
            print("Retry effect: " + json.dumps(item["analysis_report"].get("retry_effect", {}), ensure_ascii=False, sort_keys=True))
    print(f"Operational analyze report: {auto_report_path}")
    print(f"Latest analyze report: {latest_analyze_report_path}")
    return 0


def _run_migrate_command(args: argparse.Namespace) -> int:
    if not args.backfill_items_from_videos:
        print("No migrate operation selected.", file=sys.stderr)
        return 1
    db = Database(db_path=DB_PATH, schema_path="db/schema.sql")
    summary = backfill_items_from_videos(
        db=db,
        only_missing=args.only_missing,
        dry_run=args.dry_run,
    )
    write_backfill_reports(
        summary,
        audit_file=args.audit_file,
        summary_file=args.summary_file,
    )
    print("Backfill summary")
    print(f"- scanned: {summary.scanned}")
    print(f"- created: {summary.created}")
    print(f"- updated: {summary.updated}")
    print(f"- skipped_existing: {summary.skipped_existing}")
    print(f"- conflicts: {summary.conflicts}")
    print(f"- warning_count: {summary.warning_count}")
    print(f"- error_count: {summary.error_count}")
    if summary.action_counts:
        print(f"- action_counts: {json.dumps(summary.action_counts, ensure_ascii=False, sort_keys=True)}")
    if summary.conflict_type_counts:
        print(f"- conflict_type_counts: {json.dumps(summary.conflict_type_counts, ensure_ascii=False, sort_keys=True)}")
    return 0


def _run_cleanup_command(args: argparse.Namespace) -> int:
    if not args.remove_explicit_noise:
        print("No cleanup operation selected.", file=sys.stderr)
        return 1
    db = Database(db_path=DB_PATH, schema_path="db/schema.sql")
    source_ids = None
    if args.source or args.source_set:
        config = load_source_config()
        source_ids = set(
            resolve_source_ids(config, source_id=args.source, source_set=args.source_set)
        )
    cleanup_report = cleanup_explicit_noise_items(
        db,
        source_ids=source_ids,
        dry_run=args.dry_run,
    )
    reports_root = Path(args.reports_root)
    cleanup_report_path = write_report_json(
        reports_root,
        "cleanup",
        f"cleanup-report-{build_run_label(args.source, args.source_set)}",
        {
            "source": args.source,
            "source_set": args.source_set,
            "remove_explicit_noise": args.remove_explicit_noise,
            **cleanup_report,
        },
    )
    latest_cleanup_report_path = copy_to_latest(reports_root, "cleanup", cleanup_report_path)
    print("Cleanup summary")
    print(f"- dry_run: {cleanup_report['dry_run']}")
    print(f"- matched_count: {cleanup_report['matched_count']}")
    print(f"- deleted_count: {cleanup_report['deleted_count']}")
    for item in cleanup_report["matched_items"]:
        print("=" * 80)
        print(item["title"] or "(untitled)")
        print(item["url"])
        print(f"Source: {item['source_id']}")
        print(f"Body kind: {item['body_kind']}")
        print(f"Content status: {item['content_status']}")
    print(f"Cleanup report written: {cleanup_report_path}")
    print(f"Latest cleanup report: {latest_cleanup_report_path}")
    return 0


def _run_export_command(args: argparse.Namespace) -> int:
    db = Database(db_path=DB_PATH, schema_path="db/schema.sql")
    source_ids = None
    if args.source or args.source_set:
        config = load_source_config()
        source_ids = set(
            resolve_source_ids(config, source_id=args.source, source_set=args.source_set)
        )
    output_dir = Path(__file__).resolve().parent / "outputs"
    format_to_directory = {
        "reader": output_dir / "reader",
        "reader-json": output_dir / "reader",
        "notebooklm-json": output_dir / "notebooklm",
        "notebooklm-markdown": output_dir / "notebooklm",
    }
    export_result = export_items(
        db=db,
        export_format=args.format,
        output_dir=format_to_directory[args.format],
        query=args.query,
        compare=args.compare,
        source_ids=source_ids,
    )
    if args.compare:
        export_path, compare_report = export_result
    else:
        export_path = export_result
    print(f"Export written: {export_path}")
    reports_root = Path(args.reports_root)
    copied_export_path = copy_report_artifact(reports_root, "export", export_path)
    latest_export_artifact_path = copy_to_latest(reports_root, "export", copied_export_path)
    if args.compare:
        print("Compare summary")
        print(f"- items_priority_count: {compare_report.items_priority_count}")
        print(f"- legacy_fallback_count: {compare_report.legacy_fallback_count}")
        print(f"- overlapping_video_count: {compare_report.overlapping_video_count}")
        print(f"- items_priority_only_count: {compare_report.items_priority_only_count}")
        print(f"- legacy_only_count: {compare_report.legacy_only_count}")
        print(f"- changed_video_count: {compare_report.changed_video_count}")
        print("- items_priority_source_counts: " + json.dumps(compare_report.items_priority_source_counts, ensure_ascii=False, sort_keys=True))
        print("- legacy_fallback_source_counts: " + json.dumps(compare_report.legacy_fallback_source_counts, ensure_ascii=False, sort_keys=True))
        print("- source_count_diff: " + json.dumps(compare_report.source_count_diff, ensure_ascii=False, sort_keys=True))
        print("- items_priority_exclusion_reasons: " + json.dumps(compare_report.items_priority_exclusion_reasons, ensure_ascii=False, sort_keys=True))
        print("- legacy_exclusion_reasons: " + json.dumps(compare_report.legacy_exclusion_reasons, ensure_ascii=False, sort_keys=True))
        print("- exclusion_reason_diff: " + json.dumps(compare_report.exclusion_reason_diff, ensure_ascii=False, sort_keys=True))
        compare_report_path = write_report_json(
            reports_root,
            "export",
            f"export-compare-{build_run_label(args.source, args.source_set)}",
            {
                "source": args.source,
                "source_set": args.source_set,
                "format": args.format,
                "query": args.query,
                "compare": args.compare,
                "compare_report": {
                    "items_priority_count": compare_report.items_priority_count,
                    "legacy_fallback_count": compare_report.legacy_fallback_count,
                    "overlapping_video_count": compare_report.overlapping_video_count,
                    "items_priority_only_count": compare_report.items_priority_only_count,
                    "legacy_only_count": compare_report.legacy_only_count,
                    "changed_video_count": compare_report.changed_video_count,
                    "items_priority_source_counts": compare_report.items_priority_source_counts,
                    "legacy_fallback_source_counts": compare_report.legacy_fallback_source_counts,
                    "source_count_diff": compare_report.source_count_diff,
                    "items_priority_exclusion_reasons": compare_report.items_priority_exclusion_reasons,
                    "legacy_exclusion_reasons": compare_report.legacy_exclusion_reasons,
                    "exclusion_reason_diff": compare_report.exclusion_reason_diff,
                },
                "export_artifact": str(copied_export_path),
            },
        )
        latest_export_report_path = copy_to_latest(reports_root, "export", compare_report_path)
        print(f"Export compare report written: {compare_report_path}")
    else:
        export_report_path = write_report_json(
            reports_root,
            "export",
            f"export-report-{build_run_label(args.source, args.source_set)}",
            {
                "source": args.source,
                "source_set": args.source_set,
                "format": args.format,
                "query": args.query,
                "compare": args.compare,
                "export_artifact": str(copied_export_path),
            },
        )
        latest_export_report_path = copy_to_latest(reports_root, "export", export_report_path)
        print(f"Export report written: {export_report_path}")
    print(f"Dated export artifact copy: {copied_export_path}")
    print(f"Latest export artifact copy: {latest_export_artifact_path}")
    print(f"Latest export report: {latest_export_report_path}")
    return 0


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
    if args.command == "collect":
        return _run_collect_command(args)
    if args.command == "analyze":
        return _run_analyze_command(args)
    if args.command == "export":
        return _run_export_command(args)
    if args.command == "migrate":
        return _run_migrate_command(args)
    if args.command == "cleanup":
        return _run_cleanup_command(args)
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
