import json
import shutil
import unittest
from pathlib import Path
from uuid import uuid4

from config import CONFIG_DIR, load_structured_config
from db.database import Database
from db.repository import CanonicalItem, ItemRepository, SourceRecord
from evaluation.notebooklm_policy import should_include_in_notebooklm
from evaluation.quality import evaluate_quality
from pipeline.export import export_items


class QualityPolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = load_structured_config(CONFIG_DIR / "policies.yaml")
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

    def test_description_only_quality_is_warning_not_full_eligible(self) -> None:
        result = evaluate_quality(
            {
                "body_kind": "description_only",
                "content_status": "available",
                "retrieval_diagnostics": {},
                "cleaned_text": "Revenue grew strongly and margin improved across the AI business." * 4,
            },
            self.policy,
        )

        self.assertEqual(result["quality_tier"], "medium")
        self.assertEqual(result["reader_eligibility"], "eligible_with_warning")
        self.assertEqual(result["notebooklm_eligibility"], "eligible_with_warning")

    def test_metadata_only_is_ineligible_for_notebooklm(self) -> None:
        include, reason = should_include_in_notebooklm(
            {
                "body_kind": "metadata_only",
                "content_status": "unavailable",
                "quality_tier": "reject",
                "notebooklm_eligibility": "ineligible",
            },
            self.policy,
        )

        self.assertFalse(include)
        self.assertEqual(reason, "content_status_excluded")

    def test_reader_export_keeps_metadata_only_with_warning(self) -> None:
        item = CanonicalItem(
            item_id="youtube.default:meta1",
            source_id="youtube.default",
            source_type="youtube_video",
            external_id="meta1",
            title="Metadata only",
            author="Channel",
            published_at="2026-03-21T00:00:00Z",
            url="https://example.com/meta1",
            raw_text="",
            cleaned_text="",
            body_kind="metadata_only",
            content_status="unavailable",
            content_warning="Transcript unavailable.",
            evidence_strength="none",
            quality_tier="reject",
            reader_eligibility="eligible_with_warning",
            notebooklm_eligibility="ineligible",
        )
        self.repository.upsert_item(item)
        self.repository.upsert_item_summary(
            item.item_id,
            "Content unavailable",
            "Metadata only item.",
        )

        export_path = export_items(
            db=self.db,
            export_format="reader-json",
            output_dir=self.output_root,
        )
        payload = json.loads(export_path.read_text(encoding="utf-8"))

        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["results"][0]["body_kind"], "metadata_only")
        self.assertIn("content_unavailable", payload["results"][0]["reader_warning_flags"])

    def test_notebooklm_export_excludes_ineligible_items(self) -> None:
        kept = CanonicalItem(
            item_id="youtube.default:good1",
            source_id="youtube.default",
            source_type="youtube_video",
            external_id="good1",
            title="Good item",
            author="Channel",
            published_at="2026-03-21T00:00:00Z",
            url="https://example.com/good1",
            raw_text="Raw",
            cleaned_text="AI revenue growth improved enterprise demand significantly." * 6,
            body_kind="full_text",
            content_status="available",
            evidence_strength="medium",
            quality_tier="high",
            reader_eligibility="eligible",
            notebooklm_eligibility="eligible",
        )
        dropped = CanonicalItem(
            item_id="youtube.default:meta2",
            source_id="youtube.default",
            source_type="youtube_video",
            external_id="meta2",
            title="Dropped item",
            author="Channel",
            published_at="2026-03-21T00:00:00Z",
            url="https://example.com/meta2",
            raw_text="",
            cleaned_text="",
            body_kind="metadata_only",
            content_status="unavailable",
            evidence_strength="none",
            quality_tier="reject",
            reader_eligibility="eligible_with_warning",
            notebooklm_eligibility="ineligible",
        )
        self.repository.upsert_item(kept)
        self.repository.upsert_item(dropped)
        self.repository.upsert_item_summary(kept.item_id, "Good summary", "- Fact 1")
        self.repository.upsert_item_summary(dropped.item_id, "Unavailable", "No content")

        export_path = export_items(
            db=self.db,
            export_format="notebooklm-json",
            output_dir=self.output_root,
        )
        payload = json.loads(export_path.read_text(encoding="utf-8"))

        self.assertEqual(payload["stats"]["video_count"], 1)
        self.assertEqual(payload["documents"][0]["video"]["video_id"], "good1")


if __name__ == "__main__":
    unittest.main()
