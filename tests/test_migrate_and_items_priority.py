import json
import shutil
import unittest
from pathlib import Path
from uuid import uuid4

from config import CONFIG_DIR, load_structured_config
from db.database import Database
from db.repository import CanonicalItem, ItemRepository, SourceRecord
from pipeline.analyze import analyze_items, build_analysis_metrics, build_analysis_report
from pipeline.export import ExportCompareReport, export_items
from pipeline.migrate import backfill_items_from_videos, map_legacy_video_to_item, write_backfill_reports
from pipeline.retry_policy import DEFAULT_RETRY_POLICY, load_retry_policy


class MigrateAndItemsPriorityTests(unittest.TestCase):
    def setUp(self) -> None:
        project_root = Path(__file__).resolve().parents[1]
        self.schema_path = project_root / "db" / "schema.sql"
        self.temp_dir = project_root / "tests" / ".tmp"
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.temp_dir / f"{uuid4().hex}.sqlite3"
        self.db = Database(str(self.db_path), str(self.schema_path))
        self.repository = ItemRepository(self.db)
        self.repository.sync_sources(
            [
                SourceRecord(
                    source_id="youtube.default",
                    source_name="YouTube Default",
                    source_type="youtube_video",
                    base_url="https://www.youtube.com",
                    collection_method="youtube_api_search",
                    cadence_type="manual",
                    trust_level="medium",
                    notebooklm_default_policy="conditional_description_only",
                    reader_default_policy="include_with_warning",
                )
            ]
        )
        self.output_root = self.temp_dir / f"exports-{uuid4().hex}"
        self.output_root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        if self.output_root.exists():
            shutil.rmtree(self.output_root)

    def _insert_legacy_video(
        self,
        *,
        video_id: str,
        transcript_source: str,
        raw_text: str,
        cleaned_text: str,
        content_status: str,
        content_warning: str = "",
    ) -> None:
        self.db.upsert_video(
            {
                "video_id": video_id,
                "title": f"title-{video_id}",
                "channel": "Channel",
                "published_at": "2026-03-21T00:00:00Z",
                "url": f"https://example.com/{video_id}",
                "description": raw_text if transcript_source in {"description", "api_description"} else "",
                "transcript_source": transcript_source,
                "transcript_length": len(raw_text),
            }
        )
        self.db.upsert_transcript(video_id, raw_text, cleaned_text)
        self.db.update_video_content_metadata(
            video_id,
            content_status=content_status,
            content_warning=content_warning,
            metadata_only_reason="description_cleaned_empty" if transcript_source == "description" and not cleaned_text else "",
            retrieval_diagnostics={"selected_caption_source": transcript_source},
        )
        if cleaned_text:
            chunks = self.db.replace_chunks(video_id, [cleaned_text])
            self.db.upsert_chunk_summary(
                chunks[0]["chunk_id"],
                {
                    "summary": "Chunk summary",
                    "key_points": ["Point A"],
                    "entities": ["Entity A"],
                    "category": ["Category A"],
                    "signal_score": 0.7,
                },
            )
            self.db.upsert_video_summary(video_id, "Short summary", "- Point A")

    def test_migrate_dry_run_returns_estimate(self) -> None:
        self._insert_legacy_video(
            video_id="video1",
            transcript_source="manual",
            raw_text="Raw text",
            cleaned_text="Cleaned text " * 20,
            content_status="available",
        )

        summary = backfill_items_from_videos(self.db, dry_run=True)

        self.assertEqual(summary.scanned, 1)
        self.assertEqual(summary.created, 1)
        self.assertEqual(summary.action_counts["create"], 1)
        self.assertEqual(summary.error_count, 0)
        self.assertIsNone(self.repository.get_item("youtube.default:video1"))

    def test_migrate_creates_items_and_is_idempotent(self) -> None:
        self._insert_legacy_video(
            video_id="video2",
            transcript_source="manual",
            raw_text="Raw text",
            cleaned_text="Cleaned text " * 20,
            content_status="available",
        )

        first = backfill_items_from_videos(self.db)
        second = backfill_items_from_videos(self.db, only_missing=True)

        self.assertEqual(first.created, 1)
        self.assertEqual(second.skipped_existing, 1)
        self.assertEqual(second.action_counts["skip"], 1)
        item = self.repository.get_item("youtube.default:video2")
        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["body_kind"], "full_text")
        self.assertEqual(len(self.repository.get_item_chunks(item["item_id"])), 1)
        self.assertIsNotNone(self.repository.get_item_summary(item["item_id"]))

    def test_description_fallback_migrates_as_description_only(self) -> None:
        self._insert_legacy_video(
            video_id="video3",
            transcript_source="description",
            raw_text="Revenue improved across the AI segment.",
            cleaned_text="Revenue improved across the AI segment." * 4,
            content_status="available",
        )

        backfill_items_from_videos(self.db)
        item = self.repository.get_item("youtube.default:video3")
        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["body_kind"], "description_only")
        self.assertEqual(item["evidence_strength"], "weak")

    def test_metadata_only_migrates_as_notebooklm_ineligible(self) -> None:
        self._insert_legacy_video(
            video_id="video4",
            transcript_source="none",
            raw_text="",
            cleaned_text="",
            content_status="unavailable",
            content_warning="Content unavailable.",
        )

        backfill_items_from_videos(self.db)
        item = self.repository.get_item("youtube.default:video4")
        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["body_kind"], "metadata_only")
        self.assertEqual(item["notebooklm_eligibility"], "ineligible")

    def test_analyze_only_missing_picks_missing_summary_state(self) -> None:
        item = CanonicalItem(
            item_id="youtube.default:item1",
            source_id="youtube.default",
            source_type="youtube_video",
            external_id="item1",
            title="Item 1",
            author="Channel",
            published_at="2026-03-21T00:00:00Z",
            url="https://example.com/item1",
            raw_text="Raw",
            cleaned_text="Cleaned text " * 20,
            body_kind="full_text",
            content_status="available",
            evidence_strength="medium",
            quality_tier="high",
            reader_eligibility="eligible",
            notebooklm_eligibility="eligible",
        )
        self.repository.upsert_item(item)
        analyzed = analyze_items(self.db, source_id="youtube.default", only_missing=True, skip_llm=True)

        self.assertEqual(len(analyzed), 1)
        self.assertTrue(analyzed[0]["analysis_state"]["missing_summary"])
        self.assertIn("analyze.missing.summary_missing", analyzed[0]["analysis_report"]["missing_reason_codes"])

    def test_analyze_report_separates_missing_and_retry_reasons(self) -> None:
        item = CanonicalItem(
            item_id="youtube.default:item2",
            source_id="youtube.default",
            source_type="youtube_video",
            external_id="item2",
            title="Item 2",
            author="Channel",
            published_at="2026-03-21T00:00:00Z",
            url="https://example.com/item2",
            raw_text="Raw",
            cleaned_text="",
            body_kind="metadata_only",
            content_status="unavailable",
            evidence_strength="none",
            quality_tier="low",
            reader_eligibility="eligible_with_warning",
            notebooklm_eligibility="ineligible",
        )
        self.repository.upsert_item(item)

        report = build_analysis_report(
            self.repository,
            self.repository.list_items(),
            source_id="youtube.default",
            only_missing=True,
            retry_ineligible=True,
            retry_low_quality=True,
        )

        self.assertEqual(len(report), 1)
        self.assertIn("analyze.missing.cleaned_text_empty", report[0]["missing_reason_codes"])
        self.assertIn("analyze.retry.ineligible", report[0]["retry_reason_codes"])
        self.assertNotIn("analyze.retry.low_quality", report[0]["retry_reason_codes"])
        self.assertTrue(all("message" in reason for reason in report[0]["missing_reasons"]))
        self.assertTrue(report[0]["selected"])

    def test_analyze_report_without_retry_policy_matches_current_default_behavior(self) -> None:
        item = CanonicalItem(
            item_id="youtube.default:item-default",
            source_id="youtube.default",
            source_type="youtube_video",
            external_id="item-default",
            title="Item Default",
            author="Channel",
            published_at="2026-03-21T00:00:00Z",
            url="https://example.com/item-default",
            raw_text="Raw",
            cleaned_text="Cleaned text " * 20,
            body_kind="full_text",
            content_status="available",
            evidence_strength="medium",
            quality_tier="low",
            reader_eligibility="eligible",
            notebooklm_eligibility="ineligible",
        )
        self.repository.upsert_item(item)
        items = self.repository.list_items()

        report_without_policy = build_analysis_report(
            self.repository,
            items,
            source_id="youtube.default",
            only_missing=False,
            retry_ineligible=True,
            retry_low_quality=True,
        )
        report_with_default = build_analysis_report(
            self.repository,
            items,
            source_id="youtube.default",
            only_missing=False,
            retry_ineligible=True,
            retry_low_quality=True,
            retry_policy=load_retry_policy(),
        )

        self.assertEqual(
            report_without_policy[0]["retry_reason_codes"],
            ["analyze.retry.ineligible"],
        )
        self.assertEqual(report_without_policy[0]["retry_reason_codes"], report_with_default[0]["retry_reason_codes"])
        self.assertEqual(
            report_without_policy[0]["retry_policy_applied"][0]["max_retries"],
            3,
        )
        self.assertEqual(
            report_without_policy[0]["retry_policy_applied"][0]["cooldown_hours"],
            24,
        )

    def test_analyze_report_allows_retry_policy_body_kind_override(self) -> None:
        item = CanonicalItem(
            item_id="youtube.default:item-override",
            source_id="youtube.default",
            source_type="youtube_video",
            external_id="item-override",
            title="Item Override",
            author="Channel",
            published_at="2026-03-21T00:00:00Z",
            url="https://example.com/item-override",
            raw_text="Raw",
            cleaned_text="Cleaned text " * 20,
            body_kind="metadata_only",
            content_status="unavailable",
            evidence_strength="none",
            quality_tier="low",
            reader_eligibility="eligible_with_warning",
            notebooklm_eligibility="ineligible",
        )
        self.repository.upsert_item(item)

        retry_policy = {
            "reason_rules": {
                "analyze.retry.ineligible": {
                    "body_kind_overrides": {
                        "metadata_only": {
                            "enabled": False,
                        }
                    }
                },
                "analyze.retry.low_quality": {
                    "body_kind_overrides": {
                        "metadata_only": {
                            "match": {
                                "quality_tier": ["low", "reject"],
                            }
                        }
                    }
                },
            }
        }
        report = build_analysis_report(
            self.repository,
            self.repository.list_items(),
            source_id="youtube.default",
            only_missing=False,
            retry_ineligible=True,
            retry_low_quality=True,
            retry_policy=retry_policy,
        )

        self.assertEqual(report[0]["retry_reason_codes"], ["analyze.retry.low_quality"])

    def test_analyze_report_blocks_retry_when_max_retries_reached(self) -> None:
        item = CanonicalItem(
            item_id="youtube.default:item-blocked",
            source_id="youtube.default",
            source_type="youtube_video",
            external_id="item-blocked",
            title="Item Blocked",
            author="Channel",
            published_at="2026-03-21T00:00:00Z",
            url="https://example.com/item-blocked",
            raw_text="Raw",
            cleaned_text="Cleaned text " * 20,
            body_kind="full_text",
            content_status="available",
            evidence_strength="medium",
            quality_tier="low",
            reader_eligibility="eligible",
            notebooklm_eligibility="eligible",
            cleaning_diagnostics={
                "retry_policy_history": {
                    "analyze.retry.low_quality": [
                        {"attempted_at": "2026-03-19T00:00:00+00:00"},
                        {"attempted_at": "2026-03-20T00:00:00+00:00"},
                        {"attempted_at": "2026-03-21T00:00:00+00:00"},
                    ]
                }
            },
        )
        self.repository.upsert_item(item)

        report = build_analysis_report(
            self.repository,
            self.repository.list_items(),
            source_id="youtube.default",
            only_missing=False,
            retry_ineligible=False,
            retry_low_quality=True,
            retry_policy={
                "reason_rules": {
                    "analyze.retry.low_quality": {
                        "enabled": True,
                        "source_overrides": {},
                        "body_kind_overrides": {},
                    }
                }
            },
        )

        self.assertEqual(report[0]["retry_reason_codes"], [])
        self.assertEqual(
            report[0]["retry_policy_audit"][0]["blocked_reason"],
            "max_retries_reached",
        )

    def test_analyze_items_persists_retry_audit_and_quality_improved_success_metrics(self) -> None:
        item = CanonicalItem(
            item_id="text.source:item-retry",
            source_id="text.source",
            source_type="article",
            external_id="item-retry",
            title="Item Retry",
            author="Desk",
            published_at="2026-03-21T00:00:00Z",
            url="https://example.com/item-retry",
            raw_text="Raw",
            cleaned_text="Cleaned text " * 20,
            body_kind="full_text",
            content_status="available",
            evidence_strength="medium",
            quality_tier="low",
            reader_eligibility="eligible",
            notebooklm_eligibility="eligible",
        )
        self.repository.sync_sources(
            [
                SourceRecord(
                    source_id="text.source",
                    source_name="Text Source",
                    source_type="article",
                    base_url="https://example.com",
                    collection_method="manual",
                    cadence_type="manual",
                    trust_level="medium",
                    notebooklm_default_policy="conditional_description_only",
                    reader_default_policy="include_with_warning",
                )
            ]
        )
        self.repository.upsert_item(item)

        selection_rows = build_analysis_report(
            self.repository,
            self.repository.list_items(),
            source_id="text.source",
            only_missing=False,
            retry_ineligible=False,
            retry_low_quality=True,
        )
        analyzed = analyze_items(
            self.db,
            source_id="text.source",
            retry_low_quality=True,
            skip_llm=True,
            retry_policy={
                "reason_rules": {
                    "analyze.retry.low_quality": {
                        "enabled": True,
                        "source_overrides": {},
                        "body_kind_overrides": {},
                        "max_retries": 3,
                        "cooldown_hours": 0,
                    }
                }
            },
        )
        metrics = build_analysis_metrics(selection_rows, analyzed)
        stored = self.repository.get_item("text.source:item-retry")

        self.assertEqual(len(analyzed), 1)
        self.assertEqual(
            analyzed[0]["analysis_report"]["retry_policy_applied"][0]["reason_code"],
            "analyze.retry.low_quality",
        )
        self.assertTrue(analyzed[0]["analysis_report"]["retry_effect"]["quality_improved"])
        assert stored is not None
        self.assertIn("retry_policy_history", stored["cleaning_diagnostics"])
        self.assertEqual(metrics["retried_items"], 1)
        self.assertEqual(metrics["retry_success_count"], 1)
        self.assertEqual(metrics["retry_success_definition"], "quality_tier_improved")
        self.assertEqual(
            metrics["source_retry_distribution"]["text.source"]["retry_candidate_count"],
            1,
        )
        self.assertEqual(
            metrics["source_retry_distribution"]["text.source"]["retried_count"],
            1,
        )

    def test_analyze_metrics_aggregate_blocked_reasons_and_source_counts(self) -> None:
        rows = [
            {
                "item_id": "a",
                "source_id": "youtube.default",
                "selected_by_retry": False,
                "retry_candidates": [
                    {"reason_code": "analyze.retry.low_quality", "matched": True, "blocked_reason": "override_disabled"}
                ],
            },
            {
                "item_id": "b",
                "source_id": "text.source",
                "selected_by_retry": False,
                "retry_candidates": [
                    {"reason_code": "analyze.retry.low_quality", "matched": True, "blocked_reason": "max_retries_reached"}
                ],
            },
            {
                "item_id": "c",
                "source_id": "text.source",
                "selected_by_retry": False,
                "retry_candidates": [
                    {"reason_code": "analyze.retry.ineligible", "matched": True, "blocked_reason": "cooldown_active"}
                ],
            },
            {
                "item_id": "d",
                "source_id": "text.source",
                "selected_by_retry": True,
                "retry_candidates": [
                    {"reason_code": "analyze.retry.low_quality", "matched": True, "blocked_reason": None}
                ],
            },
        ]

        metrics = build_analysis_metrics(
            rows,
            [
                {
                    "item_id": "d",
                    "analysis_report": {
                        "retry_effect": {"quality_improved": True}
                    },
                }
            ],
        )

        self.assertEqual(metrics["blocked_reason_counts"]["blocked_by_override"], 1)
        self.assertEqual(metrics["blocked_reason_counts"]["blocked_by_max_retries"], 1)
        self.assertEqual(metrics["blocked_reason_counts"]["blocked_by_cooldown"], 1)
        self.assertEqual(
            metrics["source_retry_distribution"]["youtube.default"]["analyzed_count"],
            1,
        )
        self.assertEqual(
            metrics["source_retry_distribution"]["text.source"]["retry_candidate_count"],
            3,
        )
        self.assertEqual(
            metrics["source_retry_distribution"]["text.source"]["retried_count"],
            1,
        )

    def test_analyze_items_keeps_only_latest_retry_history_entries(self) -> None:
        item = CanonicalItem(
            item_id="text.source:item-history",
            source_id="text.source",
            source_type="article",
            external_id="item-history",
            title="Item History",
            author="Desk",
            published_at="2026-03-21T00:00:00Z",
            url="https://example.com/item-history",
            raw_text="Raw",
            cleaned_text="Cleaned text " * 20,
            body_kind="full_text",
            content_status="available",
            evidence_strength="medium",
            quality_tier="low",
            reader_eligibility="eligible",
            notebooklm_eligibility="eligible",
            cleaning_diagnostics={
                "retry_policy_history": {
                    "analyze.retry.low_quality": [
                        {"attempted_at": "2026-03-17T00:00:00+00:00"},
                        {"attempted_at": "2026-03-18T00:00:00+00:00"},
                    ]
                }
            },
        )
        self.repository.sync_sources(
            [
                SourceRecord(
                    source_id="text.source",
                    source_name="Text Source",
                    source_type="article",
                    base_url="https://example.com",
                    collection_method="manual",
                    cadence_type="manual",
                    trust_level="medium",
                    notebooklm_default_policy="conditional_description_only",
                    reader_default_policy="include_with_warning",
                )
            ]
        )
        self.repository.upsert_item(item)

        analyze_items(
            self.db,
            source_id="text.source",
            retry_low_quality=True,
            skip_llm=True,
            retry_policy={
                "history_limit": 2,
                "reason_rules": {
                    "analyze.retry.low_quality": {
                        "enabled": True,
                        "source_overrides": {},
                        "body_kind_overrides": {},
                        "cooldown_hours": 0,
                    }
                },
            },
        )
        stored = self.repository.get_item("text.source:item-history")

        assert stored is not None
        history = stored["cleaning_diagnostics"]["retry_policy_history"]["analyze.retry.low_quality"]
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["attempted_at"], "2026-03-18T00:00:00+00:00")

    def test_export_prefers_items_over_legacy(self) -> None:
        self._insert_legacy_video(
            video_id="video5",
            transcript_source="manual",
            raw_text="Raw text",
            cleaned_text="Legacy cleaned text " * 20,
            content_status="available",
        )
        item = CanonicalItem(
            item_id="youtube.default:video5",
            source_id="youtube.default",
            source_type="youtube_video",
            external_id="video5",
            title="Item title wins",
            author="Item Channel",
            published_at="2026-03-21T00:00:00Z",
            url="https://example.com/video5",
            raw_text="Raw item",
            cleaned_text="Item cleaned text " * 20,
            body_kind="full_text",
            content_status="available",
            evidence_strength="medium",
            quality_tier="high",
            reader_eligibility="eligible",
            notebooklm_eligibility="eligible",
        )
        self.repository.upsert_item(item)
        self.repository.upsert_item_summary(item.item_id, "Item summary", "Item detailed summary")

        export_path = export_items(self.db, "reader-json", self.output_root)
        payload = json.loads(export_path.read_text(encoding="utf-8"))

        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["results"][0]["title"], "Item title wins")

    def test_export_falls_back_to_legacy_when_items_absent(self) -> None:
        self._insert_legacy_video(
            video_id="video6",
            transcript_source="manual",
            raw_text="Raw text",
            cleaned_text="Legacy cleaned text " * 20,
            content_status="available",
        )

        export_path = export_items(self.db, "reader-json", self.output_root)
        payload = json.loads(export_path.read_text(encoding="utf-8"))

        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["results"][0]["video_id"], "video6")
        self.assertEqual(payload["results"][0]["title"], "title-video6")

    def test_migrate_audit_and_summary_files_are_written(self) -> None:
        self._insert_legacy_video(
            video_id="video7",
            transcript_source="manual",
            raw_text="Raw text",
            cleaned_text="Cleaned text " * 20,
            content_status="available",
        )
        audit_path = self.temp_dir / f"audit-{uuid4().hex}.json"
        summary_path = self.temp_dir / f"summary-{uuid4().hex}.json"

        summary = backfill_items_from_videos(self.db, dry_run=True)
        write_backfill_reports(summary, audit_file=audit_path, summary_file=summary_path)

        audit_payload = json.loads(audit_path.read_text(encoding="utf-8"))
        summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
        self.assertEqual(audit_payload[0]["action"], "create")
        self.assertEqual(audit_payload[0]["body_kind"], "full_text")
        self.assertEqual(summary_payload["action_counts"]["create"], 1)
        self.assertEqual(summary_payload["conflict_type_counts"], {})

    def test_migrate_conflict_preserves_existing_high_level_outputs(self) -> None:
        self._insert_legacy_video(
            video_id="video8",
            transcript_source="manual",
            raw_text="Legacy raw",
            cleaned_text="Legacy cleaned text " * 20,
            content_status="available",
        )
        item = CanonicalItem(
            item_id="youtube.default:video8",
            source_id="youtube.default",
            source_type="youtube_video",
            external_id="video8",
            title="Existing item",
            author="Channel",
            published_at="2026-03-21T00:00:00Z",
            url="https://example.com/video8",
            raw_text="Existing raw",
            cleaned_text="Existing cleaned text " * 20,
            body_kind="full_text",
            content_status="available",
            evidence_strength="medium",
            quality_tier="high",
            reader_eligibility="eligible",
            notebooklm_eligibility="eligible",
        )
        self.repository.upsert_item(item)
        self.repository.upsert_item_summary(item.item_id, "Existing summary", "Existing detail")

        summary = backfill_items_from_videos(self.db)

        self.assertEqual(summary.conflicts, 1)
        self.assertEqual(summary.action_counts["conflict"], 1)
        self.assertEqual(summary.conflict_type_counts["migrate.conflict.item_summary_diff"], 1)
        stored_summary = self.repository.get_item_summary(item.item_id)
        assert stored_summary is not None
        self.assertEqual(stored_summary["short_summary"], "Existing summary")

    def test_export_compare_reports_priority_vs_legacy_diff(self) -> None:
        self._insert_legacy_video(
            video_id="video9",
            transcript_source="manual",
            raw_text="Raw text",
            cleaned_text="Legacy cleaned text " * 20,
            content_status="available",
        )
        item = CanonicalItem(
            item_id="youtube.default:video9",
            source_id="youtube.default",
            source_type="youtube_video",
            external_id="video9",
            title="Items win",
            author="Channel",
            published_at="2026-03-21T00:00:00Z",
            url="https://example.com/video9",
            raw_text="Raw text",
            cleaned_text="Item cleaned text " * 20,
            body_kind="full_text",
            content_status="available",
            evidence_strength="medium",
            quality_tier="high",
            reader_eligibility="eligible",
            notebooklm_eligibility="eligible",
        )
        self.repository.upsert_item(item)
        self.repository.upsert_item_summary(item.item_id, "Item summary", "Item detail")

        export_path, compare_report = export_items(
            self.db,
            "reader-json",
            self.output_root,
            compare=True,
        )

        self.assertTrue(export_path.exists())
        self.assertIsInstance(compare_report, ExportCompareReport)
        self.assertEqual(compare_report.items_priority_count, 1)
        self.assertEqual(compare_report.legacy_fallback_count, 1)
        self.assertEqual(compare_report.changed_video_count, 1)
        self.assertEqual(compare_report.items_priority_source_counts["youtube.default"], 1)
        self.assertEqual(compare_report.legacy_fallback_source_counts["youtube.default"], 1)
        self.assertEqual(compare_report.source_count_diff["youtube.default"], 0)


if __name__ == "__main__":
    unittest.main()
