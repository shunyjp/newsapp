import unittest
from unittest.mock import MagicMock, patch

import requests

from pipeline.metadata_only_report import build_metadata_only_report, classify_metadata_only_row
from pipeline.pipeline import NewsPipeline
from youtube.fetch_transcript import get_transcript


class MetadataOnlyReportTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
