import unittest
from pathlib import Path
from uuid import uuid4

from db.database import Database
from db.repository import CanonicalItem, ItemRepository, SourceRecord


class ItemRepositoryTests(unittest.TestCase):
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

    def test_upsert_item_is_idempotent(self) -> None:
        item = CanonicalItem(
            item_id="youtube.default:abc123",
            source_id="youtube.default",
            source_type="youtube_video",
            external_id="abc123",
            title="First title",
            author="Channel A",
            published_at="2026-03-21T00:00:00Z",
            url="https://example.com/watch?v=abc123",
            raw_text="Raw text",
            cleaned_text="Cleaned text",
            body_kind="full_text",
            content_status="available",
            evidence_strength="medium",
        )

        self.repository.upsert_item(item)
        item.title = "Updated title"
        self.repository.upsert_item(item)

        stored = self.repository.get_item(item.item_id)
        self.assertIsNotNone(stored)
        assert stored is not None
        self.assertEqual(stored["title"], "Updated title")
        self.assertEqual(stored["external_id"], "abc123")


if __name__ == "__main__":
    unittest.main()
