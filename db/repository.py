import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from db.database import Database


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass(slots=True)
class SourceRecord:
    source_id: str
    source_name: str
    source_type: str
    collection_method: str
    cadence_type: str
    trust_level: str
    notebooklm_default_policy: str
    reader_default_policy: str
    base_url: str | None = None
    priority: int = 100
    is_active: bool = True
    notes: str | None = None


@dataclass(slots=True)
class CanonicalItem:
    item_id: str
    source_id: str
    source_type: str
    url: str
    body_kind: str
    content_status: str
    evidence_strength: str
    external_id: str | None = None
    title: str | None = None
    author: str | None = None
    published_at: str | None = None
    raw_text: str = ""
    cleaned_text: str = ""
    content_warning: str | None = None
    retrieval_diagnostics: dict[str, Any] = field(default_factory=dict)
    language: str | None = None
    trust_level: str | None = None
    quality_tier: str | None = None
    reader_eligibility: str | None = None
    notebooklm_eligibility: str | None = None
    cleaning_version: str | None = None
    cleaning_diagnostics: dict[str, Any] = field(default_factory=dict)
    created_at: str | None = None
    updated_at: str | None = None


class ItemRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def sync_sources(self, sources: list[SourceRecord]) -> None:
        with self.db._connect() as connection:
            for source in sources:
                connection.execute(
                    """
                    INSERT INTO sources (
                        source_id,
                        source_name,
                        source_type,
                        base_url,
                        collection_method,
                        cadence_type,
                        priority,
                        trust_level,
                        notebooklm_default_policy,
                        reader_default_policy,
                        is_active,
                        notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source_id) DO UPDATE SET
                        source_name = excluded.source_name,
                        source_type = excluded.source_type,
                        base_url = excluded.base_url,
                        collection_method = excluded.collection_method,
                        cadence_type = excluded.cadence_type,
                        priority = excluded.priority,
                        trust_level = excluded.trust_level,
                        notebooklm_default_policy = excluded.notebooklm_default_policy,
                        reader_default_policy = excluded.reader_default_policy,
                        is_active = excluded.is_active,
                        notes = excluded.notes
                    """,
                    (
                        source.source_id,
                        source.source_name,
                        source.source_type,
                        source.base_url,
                        source.collection_method,
                        source.cadence_type,
                        source.priority,
                        source.trust_level,
                        source.notebooklm_default_policy,
                        source.reader_default_policy,
                        1 if source.is_active else 0,
                        source.notes,
                    ),
                )

    def upsert_item(self, item: CanonicalItem) -> None:
        now = utc_now_iso()
        created_at = item.created_at or now
        updated_at = item.updated_at or now
        with self.db._connect() as connection:
            connection.execute(
                """
                INSERT INTO items (
                    item_id,
                    source_id,
                    source_type,
                    external_id,
                    title,
                    author,
                    published_at,
                    url,
                    body_kind,
                    content_status,
                    content_warning,
                    retrieval_diagnostics,
                    language,
                    trust_level,
                    evidence_strength,
                    quality_tier,
                    reader_eligibility,
                    notebooklm_eligibility,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(item_id) DO UPDATE SET
                    source_id = excluded.source_id,
                    source_type = excluded.source_type,
                    external_id = excluded.external_id,
                    title = excluded.title,
                    author = excluded.author,
                    published_at = excluded.published_at,
                    url = excluded.url,
                    body_kind = excluded.body_kind,
                    content_status = excluded.content_status,
                    content_warning = excluded.content_warning,
                    retrieval_diagnostics = excluded.retrieval_diagnostics,
                    language = excluded.language,
                    trust_level = excluded.trust_level,
                    evidence_strength = excluded.evidence_strength,
                    quality_tier = excluded.quality_tier,
                    reader_eligibility = excluded.reader_eligibility,
                    notebooklm_eligibility = excluded.notebooklm_eligibility,
                    updated_at = excluded.updated_at
                """,
                (
                    item.item_id,
                    item.source_id,
                    item.source_type,
                    item.external_id,
                    item.title,
                    item.author,
                    item.published_at,
                    item.url,
                    item.body_kind,
                    item.content_status,
                    item.content_warning,
                    json.dumps(item.retrieval_diagnostics, ensure_ascii=False),
                    item.language,
                    item.trust_level,
                    item.evidence_strength,
                    item.quality_tier,
                    item.reader_eligibility,
                    item.notebooklm_eligibility,
                    created_at,
                    updated_at,
                ),
            )
            connection.execute(
                """
                INSERT INTO item_contents (
                    item_id,
                    raw_text,
                    cleaned_text,
                    cleaning_version,
                    cleaning_diagnostics
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(item_id) DO UPDATE SET
                    raw_text = excluded.raw_text,
                    cleaned_text = excluded.cleaned_text,
                    cleaning_version = excluded.cleaning_version,
                    cleaning_diagnostics = excluded.cleaning_diagnostics
                """,
                (
                    item.item_id,
                    item.raw_text,
                    item.cleaned_text,
                    item.cleaning_version,
                    json.dumps(item.cleaning_diagnostics, ensure_ascii=False),
                ),
            )

    def replace_chunks(self, item_id: str, chunks: list[str]) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        with self.db._connect() as connection:
            existing_rows = connection.execute(
                "SELECT chunk_id FROM item_chunks WHERE item_id = ?",
                (item_id,),
            ).fetchall()
            existing_chunk_ids = [row["chunk_id"] for row in existing_rows]
            if existing_chunk_ids:
                placeholders = ",".join("?" for _ in existing_chunk_ids)
                connection.execute(
                    f"DELETE FROM item_chunk_summaries WHERE chunk_id IN ({placeholders})",
                    existing_chunk_ids,
                )
            connection.execute("DELETE FROM item_chunks WHERE item_id = ?", (item_id,))
            for index, text in enumerate(chunks, start=1):
                chunk_id = f"{item_id}_{index:04d}"
                connection.execute(
                    """
                    INSERT INTO item_chunks (chunk_id, item_id, chunk_no, text)
                    VALUES (?, ?, ?, ?)
                    """,
                    (chunk_id, item_id, index, text),
                )
                records.append(
                    {
                        "chunk_id": chunk_id,
                        "item_id": item_id,
                        "chunk_no": index,
                        "text": text,
                    }
                )
        return records

    def upsert_chunk_summary(self, chunk_id: str, summary: dict[str, Any]) -> None:
        with self.db._connect() as connection:
            connection.execute(
                """
                INSERT INTO item_chunk_summaries (
                    chunk_id,
                    summary,
                    key_points_json,
                    entities_json,
                    category_json,
                    signal_score
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(chunk_id) DO UPDATE SET
                    summary = excluded.summary,
                    key_points_json = excluded.key_points_json,
                    entities_json = excluded.entities_json,
                    category_json = excluded.category_json,
                    signal_score = excluded.signal_score
                """,
                (
                    chunk_id,
                    summary.get("summary", ""),
                    json.dumps(summary.get("key_points", []), ensure_ascii=False),
                    json.dumps(summary.get("entities", []), ensure_ascii=False),
                    json.dumps(summary.get("category", []), ensure_ascii=False),
                    float(summary.get("signal_score", 0.0)),
                ),
            )

    def upsert_item_summary(
        self,
        item_id: str,
        short_summary: str,
        detailed_summary: str,
        summary_version: str = "v1",
    ) -> None:
        with self.db._connect() as connection:
            connection.execute(
                """
                INSERT INTO item_summaries (item_id, short_summary, detailed_summary, summary_version)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(item_id) DO UPDATE SET
                    short_summary = excluded.short_summary,
                    detailed_summary = excluded.detailed_summary,
                    summary_version = excluded.summary_version
                """,
                (item_id, short_summary, detailed_summary, summary_version),
            )

    def item_exists(self, item_id: str) -> bool:
        with self.db._connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM items WHERE item_id = ?",
                (item_id,),
            ).fetchone()
        return row is not None

    def get_item(self, item_id: str) -> dict[str, Any] | None:
        with self.db._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    i.*,
                    ic.raw_text,
                    ic.cleaned_text,
                    ic.cleaning_version,
                    ic.cleaning_diagnostics
                FROM items i
                LEFT JOIN item_contents ic ON ic.item_id = i.item_id
                WHERE i.item_id = ?
                """,
                (item_id,),
            ).fetchone()
        if not row:
            return None
        item = dict(row)
        for key in ("retrieval_diagnostics", "cleaning_diagnostics"):
            try:
                item[key] = json.loads(item.get(key) or "{}")
            except json.JSONDecodeError:
                item[key] = {}
        return item

    def get_item_chunks(self, item_id: str) -> list[dict[str, Any]]:
        with self.db._connect() as connection:
            rows = connection.execute(
                """
                SELECT chunk_id, item_id, chunk_no, text
                FROM item_chunks
                WHERE item_id = ?
                ORDER BY chunk_no
                """,
                (item_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_item_chunk_summaries(self, item_id: str) -> dict[str, dict[str, Any]]:
        with self.db._connect() as connection:
            rows = connection.execute(
                """
                SELECT ics.chunk_id, ics.summary, ics.key_points_json, ics.entities_json, ics.category_json, ics.signal_score
                FROM item_chunk_summaries ics
                INNER JOIN item_chunks ic ON ic.chunk_id = ics.chunk_id
                WHERE ic.item_id = ?
                """,
                (item_id,),
            ).fetchall()
        results: dict[str, dict[str, Any]] = {}
        for row in rows:
            results[row["chunk_id"]] = {
                "summary": row["summary"] or "",
                "key_points": json.loads(row["key_points_json"] or "[]"),
                "entities": json.loads(row["entities_json"] or "[]"),
                "category": json.loads(row["category_json"] or "[]"),
                "signal_score": float(row["signal_score"] or 0.0),
            }
        return results

    def get_item_summary(self, item_id: str) -> dict[str, Any] | None:
        with self.db._connect() as connection:
            row = connection.execute(
                """
                SELECT short_summary, detailed_summary, summary_version
                FROM item_summaries
                WHERE item_id = ?
                """,
                (item_id,),
            ).fetchone()
        return dict(row) if row else None

    def list_items(self) -> list[dict[str, Any]]:
        with self.db._connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    i.*,
                    ic.raw_text,
                    ic.cleaned_text,
                    ic.cleaning_version,
                    ic.cleaning_diagnostics,
                    s.short_summary,
                    s.detailed_summary,
                    s.summary_version
                FROM items i
                LEFT JOIN item_contents ic ON ic.item_id = i.item_id
                LEFT JOIN item_summaries s ON s.item_id = i.item_id
                ORDER BY COALESCE(i.published_at, ''), i.item_id
                """
            ).fetchall()
        results: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            for key in ("retrieval_diagnostics", "cleaning_diagnostics"):
                try:
                    item[key] = json.loads(item.get(key) or "{}")
                except json.JSONDecodeError:
                    item[key] = {}
            results.append(item)
        return results

    def delete_items(self, item_ids: list[str]) -> int:
        normalized_ids = [str(item_id) for item_id in item_ids if str(item_id)]
        if not normalized_ids:
            return 0
        with self.db._connect() as connection:
            placeholders = ",".join("?" for _ in normalized_ids)
            cursor = connection.execute(
                f"DELETE FROM items WHERE item_id IN ({placeholders})",
                normalized_ids,
            )
        return int(cursor.rowcount or 0)


def source_record_from_dict(data: dict[str, Any]) -> SourceRecord:
    return SourceRecord(
        source_id=str(data["source_id"]),
        source_name=str(data["source_name"]),
        source_type=str(data["source_type"]),
        base_url=data.get("base_url"),
        collection_method=str(data["collection_method"]),
        cadence_type=str(data["cadence_type"]),
        priority=int(data.get("priority", 100)),
        trust_level=str(data["trust_level"]),
        notebooklm_default_policy=str(data["notebooklm_default_policy"]),
        reader_default_policy=str(data["reader_default_policy"]),
        is_active=bool(data.get("is_active", True)),
        notes=data.get("notes"),
    )
