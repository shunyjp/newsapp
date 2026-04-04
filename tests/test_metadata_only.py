import unittest
from unittest.mock import MagicMock, patch
from pathlib import Path
from uuid import uuid4
import sys

VENDOR_DIR = Path(__file__).resolve().parents[1] / ".vendor"
if VENDOR_DIR.exists():
    sys.path.insert(0, str(VENDOR_DIR))

import requests

from db.database import Database
from main import _build_retry_metadata_only_summary, _select_retryable_metadata_only_videos
from pipeline.metadata_only_report import (
    build_metadata_only_report,
    classify_metadata_only_row,
    classify_retry_policy,
)
from pipeline.pipeline import NewsPipeline
from youtube.fetch_transcript import get_transcript


class MetadataOnlyReportTests(unittest.TestCase):
    def test_build_retry_metadata_only_summary_counts_recovery_results(self) -> None:
        summary = _build_retry_metadata_only_summary(
            [
                {"video_id": "v1", "content_status": "available"},
                {"video_id": "v2", "content_status": "unavailable"},
                {"video_id": "v3", "content_status": "available"},
            ]
        )

        self.assertEqual(
            summary,
            {
                "total": 3,
                "recovered": 2,
                "still_unavailable": 1,
                "other": 0,
            },
        )

    def test_select_retryable_metadata_only_videos_filters_and_limits_rows(self) -> None:
        videos = _select_retryable_metadata_only_videos(
            [
                {
                    "video_id": "v1",
                    "title": "Retry me",
                    "channel": "Channel A",
                    "published_at": "2026-03-21T00:00:00Z",
                    "url": "https://example.com/v1",
                    "description": "desc",
                    "metadata_only_reason": "watch_page_request_failed",
                },
                {
                    "video_id": "v2",
                    "title": "Do not retry",
                    "channel": "Channel B",
                    "published_at": "2026-03-21T00:00:00Z",
                    "url": "https://example.com/v2",
                    "description": "",
                    "metadata_only_reason": "no_caption_tracks_and_description_empty",
                },
            ],
            limit=1,
        )

        self.assertEqual(
            videos,
            [
                {
                    "video_id": "v1",
                    "title": "Retry me",
                    "channel": "Channel A",
                    "published_at": "2026-03-21T00:00:00Z",
                    "url": "https://example.com/v1",
                    "description": "desc",
                }
            ],
        )

    def test_classify_retry_policy_uses_reason_mapping(self) -> None:
        self.assertEqual(classify_retry_policy("watch_page_request_failed"), "retryable")
        self.assertEqual(
            classify_retry_policy("no_caption_tracks_and_description_empty"),
            "non_retryable",
        )
        self.assertEqual(classify_retry_policy("description_cleaned_empty"), "review_needed")

    def test_classify_prefers_persisted_runtime_reason(self) -> None:
        reason = classify_metadata_only_row(
            {
                "metadata_only_reason": "watch_page_request_failed",
                "transcript_source": "none",
                "description": "",
                "raw_text": "",
            }
        )
        self.assertEqual(reason, "watch_page_request_failed")

    def test_classify_no_retrievable_content(self) -> None:
        reason = classify_metadata_only_row(
            {
                "transcript_source": "none",
                "description": "",
                "raw_text": "",
            }
        )
        self.assertEqual(reason, "no_retrievable_content")

    def test_classify_description_cleaned_empty(self) -> None:
        reason = classify_metadata_only_row(
            {
                "transcript_source": "api_description",
                "description": "Links below",
                "raw_text": "Links below",
            }
        )
        self.assertEqual(reason, "description_cleaned_empty")

    def test_report_counts_rows_by_reason(self) -> None:
        report = build_metadata_only_report(
            [
                {
                    "video_id": "v1",
                    "title": "No content",
                    "channel": "Channel A",
                    "published_at": "2026-03-21T00:00:00Z",
                    "url": "https://example.com/v1",
                    "transcript_source": "none",
                    "transcript_length": 0,
                    "description": "",
                    "raw_text": "",
                },
                {
                    "video_id": "v2",
                    "title": "Description only",
                    "channel": "Channel B",
                    "published_at": "2026-03-21T00:00:00Z",
                    "url": "https://example.com/v2",
                    "transcript_source": "description",
                    "transcript_length": 12,
                    "description": "subscribe now",
                    "raw_text": "subscribe now",
                },
            ]
        )

        self.assertEqual(report["total"], 2)
        self.assertEqual(report["counts"]["no_retrievable_content"], 1)
        self.assertEqual(report["counts"]["description_cleaned_empty"], 1)

    def test_report_includes_persisted_diagnostics(self) -> None:
        report = build_metadata_only_report(
            [
                {
                    "video_id": "v1",
                    "title": "No content",
                    "transcript_source": "none",
                    "metadata_only_reason": "watch_page_request_failed",
                    "retrieval_diagnostics": {
                        "watch_html": "request_failed",
                        "failure_reason": "watch_page_request_failed",
                    },
                }
            ]
        )

        row = report["rows"][0]
        self.assertEqual(report["counts"]["watch_page_request_failed"], 1)
        self.assertEqual(row["reason"], "watch_page_request_failed")
        self.assertEqual(
            row["retrieval_diagnostics"]["failure_reason"],
            "watch_page_request_failed",
        )
        self.assertEqual(row["retry_policy"], "retryable")
        self.assertEqual(report["source_counts"]["none"], 1)
        self.assertEqual(report["retry_policy_counts"]["retryable"], 1)
        self.assertEqual(report["diagnostics_counts"]["watch_html"]["request_failed"], 1)
        self.assertEqual(report["reason_examples"]["watch_page_request_failed"][0]["video_id"], "v1")
        self.assertEqual(report["retry_policy_examples"]["retryable"][0]["video_id"], "v1")

    def test_report_aggregates_diagnostic_steps_and_warnings(self) -> None:
        report = build_metadata_only_report(
            [
                {
                    "video_id": "v1",
                    "title": "Watch failed",
                    "transcript_source": "none",
                    "content_warning": "watch failed",
                    "metadata_only_reason": "watch_page_request_failed",
                    "retrieval_diagnostics": {
                        "watch_html": "request_failed",
                        "player_response": "not_attempted",
                        "caption_tracks": "not_attempted",
                        "selected_caption_source": "none",
                        "caption_fetch": "not_attempted",
                        "description": "not_available",
                    },
                },
                {
                    "video_id": "v2",
                    "title": "No tracks",
                    "transcript_source": "none",
                    "content_warning": "no tracks",
                    "metadata_only_reason": "no_caption_tracks_and_description_empty",
                    "retrieval_diagnostics": {
                        "watch_html": "ok",
                        "player_response": "ok",
                        "caption_tracks": "missing",
                        "selected_caption_source": "none",
                        "caption_fetch": "not_attempted",
                        "description": "empty",
                    },
                },
            ]
        )

        self.assertEqual(report["warning_counts"]["watch failed"], 1)
        self.assertEqual(report["warning_counts"]["no tracks"], 1)
        self.assertEqual(report["retry_policy_counts"]["retryable"], 1)
        self.assertEqual(report["retry_policy_counts"]["non_retryable"], 1)
        self.assertEqual(report["diagnostics_counts"]["watch_html"]["request_failed"], 1)
        self.assertEqual(report["diagnostics_counts"]["watch_html"]["ok"], 1)
        self.assertEqual(report["diagnostics_counts"]["caption_tracks"]["missing"], 1)
        self.assertEqual(report["diagnostics_counts"]["description"]["empty"], 1)
        self.assertEqual(len(report["reason_examples"]["watch_page_request_failed"]), 1)
        self.assertEqual(
            report["reason_examples"]["no_caption_tracks_and_description_empty"][0]["video_id"],
            "v2",
        )
        self.assertEqual(report["retry_policy_examples"]["retryable"][0]["video_id"], "v1")
        self.assertEqual(report["retry_policy_examples"]["non_retryable"][0]["video_id"], "v2")


class TranscriptDiagnosticsTests(unittest.TestCase):
    @patch("youtube.fetch_transcript._get_watch_html")
    def test_watch_failure_sets_specific_reason(self, mock_get_watch_html: MagicMock) -> None:
        mock_get_watch_html.side_effect = requests.RequestException("boom")

        payload = get_transcript("video-1")

        self.assertEqual(payload["source"], "none")
        self.assertEqual(payload["diagnostics"]["failure_reason"], "watch_page_request_failed")


class PipelineWarningTests(unittest.TestCase):
    def test_description_only_empty_content_uses_specific_warning(self) -> None:
        pipeline = NewsPipeline(db=MagicMock(), skip_llm=True)

        warning = pipeline._build_content_warning("description_cleaned_empty")

        self.assertIn("cleaning removed all usable content", warning)


class PipelineRetryTests(unittest.TestCase):
    def test_run_with_videos_bypasses_skip_existing_when_requested(self) -> None:
        db = MagicMock()
        db.video_exists.return_value = True
        pipeline = NewsPipeline(db=db, skip_llm=True, skip_existing_videos=True)
        pipeline._process_video = MagicMock(  # type: ignore[method-assign]
            return_value={"video_id": "video-1"}
        )

        results = pipeline.run_with_videos(
            [
                {
                    "video_id": "video-1",
                    "title": "Example",
                    "channel": "Channel",
                    "published_at": "2026-03-21T00:00:00Z",
                    "url": "https://example.com/video-1",
                    "description": "",
                }
            ],
            apply_skip_existing=False,
        )

        self.assertEqual(results, [{"video_id": "video-1"}])
        pipeline._process_video.assert_called_once()


class DatabasePersistenceTests(unittest.TestCase):
    def test_metadata_only_runtime_diagnostics_are_persisted(self) -> None:
        project_root = Path(__file__).resolve().parents[1]
        schema_path = project_root / "db" / "schema.sql"
        temp_dir = project_root / "tests" / ".tmp"
        temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = temp_dir / f"{uuid4().hex}.sqlite3"
        db = Database(str(db_path), str(schema_path))
        db.upsert_video(
            {
                "video_id": "video-1",
                "title": "Example",
                "channel": "Channel",
                "published_at": "2026-03-21T00:00:00Z",
                "url": "https://example.com/video-1",
                "description": "",
                "transcript_source": "none",
                "transcript_length": 0,
            }
        )
        db.upsert_transcript("video-1", "", "")
        db.update_video_content_metadata(
            "video-1",
            content_status="unavailable",
            content_warning="Content unavailable: watch page request failed and API description was empty.",
            metadata_only_reason="watch_page_request_failed",
            retrieval_diagnostics={
                "watch_html": "request_failed",
                "failure_reason": "watch_page_request_failed",
            },
        )

        rows = db.get_metadata_only_rows()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["content_status"], "unavailable")
        self.assertEqual(rows[0]["metadata_only_reason"], "watch_page_request_failed")
        self.assertEqual(
            rows[0]["retrieval_diagnostics"]["failure_reason"],
            "watch_page_request_failed",
        )


if __name__ == "__main__":
    unittest.main()
