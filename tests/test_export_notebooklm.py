import json
import unittest
from pathlib import Path
import shutil

from outputs.export_notebooklm import export_notebooklm_json, export_notebooklm_markdown


class NotebookLMExportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.results = [
            {
                "video_id": "v1",
                "title": "Available Video",
                "channel": "Channel A",
                "published_at": "2026-03-21T00:00:00Z",
                "url": "https://example.com/v1",
                "transcript_source": "manual",
                "transcript_length": 1200,
                "content_status": "available",
                "content_warning": "",
                "signal_score": 0.9,
                "short_summary": "Short summary",
                "detailed_summary": "- Point 1\n- Point 2",
                "reader_points": ["Point 1"],
                "why_it_matters": "Important because...",
                "aggregated_key_points": ["Point 1"],
                "aggregated_entities": ["Entity 1"],
                "aggregated_categories": ["Category 1"],
                "chunk_summaries": [
                    {
                        "chunk_id": "v1_0001",
                        "chunk_no": 1,
                        "text": "Chunk text here",
                        "summary": "Chunk summary",
                        "key_points": ["Chunk point"],
                        "entities": ["Chunk entity"],
                        "category": ["Chunk category"],
                        "signal_score": 0.75,
                    }
                ],
                "cleaned_text": "Cleaned text here",
            },
            {
                "video_id": "v2",
                "title": "Unavailable Video",
                "channel": "Channel B",
                "published_at": "2026-03-21T00:00:00Z",
                "url": "https://example.com/v2",
                "transcript_source": "none",
                "transcript_length": 0,
                "content_status": "unavailable",
                "content_warning": "Content unavailable: transcript and description could not be retrieved.",
                "signal_score": 0.0,
                "short_summary": "Content unavailable: transcript and description could not be retrieved.",
                "detailed_summary": "Metadata only.",
                "reader_points": [],
                "why_it_matters": "Content unavailable: transcript and description could not be retrieved.",
                "aggregated_key_points": [],
                "aggregated_entities": [],
                "aggregated_categories": [],
                "chunk_summaries": [],
                "cleaned_text": "",
            },
        ]
        self.tmp_root = Path("tests/.tmp/notebooklm_export")
        if self.tmp_root.exists():
            shutil.rmtree(self.tmp_root)
        self.tmp_root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        if self.tmp_root.exists():
            shutil.rmtree(self.tmp_root)

    def test_json_export_uses_stable_document_schema(self) -> None:
        output_path = export_notebooklm_json(
            self.results,
            output_dir=self.tmp_root,
            query="AI news",
        )
        payload = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(payload["schema_version"], "notebooklm-pack.v1")
        self.assertEqual(payload["stats"]["video_count"], 2)
        self.assertEqual(payload["stats"]["metadata_only_count"], 1)
        self.assertEqual(payload["documents"][0]["video"]["video_id"], "v1")
        self.assertEqual(payload["documents"][0]["evidence"]["chunk_count"], 1)
        self.assertEqual(
            payload["documents"][0]["evidence"]["chunks"][0]["categories"],
            ["Chunk category"],
        )
        self.assertTrue(payload["documents"][1]["retrieval"]["is_metadata_only"])

    def test_markdown_export_includes_chunk_evidence_and_metadata_only_note(self) -> None:
        output_path = export_notebooklm_markdown(
            self.results,
            output_dir=self.tmp_root,
            query="AI news",
        )
        content = output_path.read_text(encoding="utf-8")

        self.assertIn("# NotebookLM Knowledge Pack", content)
        self.assertIn("## Video 1: Available Video", content)
        self.assertIn("#### Chunk 1", content)
        self.assertIn("Chunk text here", content)
        self.assertIn("## Video 2: Unavailable Video [Metadata Only]", content)
        self.assertIn(
            "Content unavailable: transcript and description could not be retrieved.",
            content,
        )

    def test_json_export_normalizes_nullable_and_scalar_fields(self) -> None:
        results = [
            {
                "video_id": "v3",
                "title": "Normalization Check",
                "channel": None,
                "published_at": None,
                "url": "https://example.com/v3",
                "transcript_source": None,
                "transcript_length": "17",
                "content_status": "available",
                "content_warning": None,
                "signal_score": "0.4",
                "short_summary": None,
                "detailed_summary": None,
                "reader_points": "Single point",
                "why_it_matters": None,
                "aggregated_key_points": ("Point A", "", None),
                "aggregated_entities": None,
                "aggregated_categories": "Category A",
                "chunk_summaries": [
                    {
                        "chunk_id": "v3_0001",
                        "chunk_no": "1",
                        "text": None,
                        "summary": None,
                        "key_points": "Chunk point",
                        "entities": None,
                        "category": ("Category 1", None),
                        "signal_score": "0.25",
                    }
                ],
                "cleaned_text": None,
            }
        ]

        output_path = export_notebooklm_json(
            results,
            output_dir=self.tmp_root,
            query="AI news",
        )
        payload = json.loads(output_path.read_text(encoding="utf-8"))
        document = payload["documents"][0]

        self.assertEqual(document["video"]["channel"], "")
        self.assertEqual(document["retrieval"]["transcript_source"], "")
        self.assertEqual(document["retrieval"]["transcript_length"], 17)
        self.assertEqual(document["summary"]["short_summary"], "No summary available.")
        self.assertEqual(document["analysis"]["reader_points"], ["Single point"])
        self.assertEqual(document["analysis"]["aggregated_key_points"], ["Point A"])
        self.assertEqual(document["analysis"]["aggregated_entities"], [])
        self.assertEqual(document["analysis"]["aggregated_categories"], ["Category A"])
        self.assertEqual(document["evidence"]["chunks"][0]["chunk_no"], 1)
        self.assertEqual(document["evidence"]["chunks"][0]["key_points"], ["Chunk point"])
        self.assertEqual(document["evidence"]["chunks"][0]["entities"], [])
        self.assertEqual(document["evidence"]["chunks"][0]["categories"], ["Category 1"])
        self.assertEqual(document["evidence"]["chunks"][0]["signal_score"], 0.25)


if __name__ == "__main__":
    unittest.main()
