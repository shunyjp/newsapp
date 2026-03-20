import json
import sqlite3
from pathlib import Path
from typing import Any


class Database:
    def __init__(self, db_path: str, schema_path: str) -> None:
        self.db_path = Path(db_path)
        self.schema_path = Path(schema_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        schema_sql = self.schema_path.read_text(encoding="utf-8")
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL;")
            connection.execute("PRAGMA synchronous = NORMAL;")
            connection.executescript(schema_sql)

    def upsert_video(self, video: dict[str, Any]) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO videos (
                    video_id,
                    title,
                    channel,
                    published_at,
                    url,
                    description,
                    transcript_source,
                    transcript_length
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(video_id) DO UPDATE SET
                    title = excluded.title,
                    channel = excluded.channel,
                    published_at = excluded.published_at,
                    url = excluded.url,
                    description = excluded.description,
                    transcript_source = COALESCE(excluded.transcript_source, videos.transcript_source),
                    transcript_length = COALESCE(excluded.transcript_length, videos.transcript_length)
                """,
                (
                    video["video_id"],
                    video.get("title"),
                    video.get("channel"),
                    video.get("published_at"),
                    video.get("url"),
                    video.get("description"),
                    video.get("transcript_source"),
                    video.get("transcript_length"),
                ),
            )

    def update_video_transcript_metadata(
        self, video_id: str, transcript_source: str, transcript_length: int
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE videos
                SET transcript_source = ?, transcript_length = ?
                WHERE video_id = ?
                """,
                (transcript_source, transcript_length, video_id),
            )

    def upsert_transcript(self, video_id: str, raw_text: str, cleaned_text: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO transcripts (video_id, raw_text, cleaned_text)
                VALUES (?, ?, ?)
                ON CONFLICT(video_id) DO UPDATE SET
                    raw_text = excluded.raw_text,
                    cleaned_text = excluded.cleaned_text
                """,
                (video_id, raw_text, cleaned_text),
            )

    def replace_chunks(self, video_id: str, chunks: list[str]) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        with self._connect() as connection:
            existing_rows = connection.execute(
                "SELECT chunk_id FROM chunks WHERE video_id = ?",
                (video_id,),
            ).fetchall()
            existing_chunk_ids = [row["chunk_id"] for row in existing_rows]
            if existing_chunk_ids:
                placeholders = ",".join("?" for _ in existing_chunk_ids)
                connection.execute(
                    f"DELETE FROM chunk_summaries WHERE chunk_id IN ({placeholders})",
                    existing_chunk_ids,
                )
            connection.execute("DELETE FROM chunks WHERE video_id = ?", (video_id,))

            for index, text in enumerate(chunks, start=1):
                chunk_id = f"{video_id}_{index:04d}"
                connection.execute(
                    """
                    INSERT INTO chunks (chunk_id, video_id, chunk_no, text)
                    VALUES (?, ?, ?, ?)
                    """,
                    (chunk_id, video_id, index, text),
                )
                records.append(
                    {
                        "chunk_id": chunk_id,
                        "video_id": video_id,
                        "chunk_no": index,
                        "text": text,
                    }
                )
        return records

    def upsert_chunk_summary(self, chunk_id: str, summary: dict[str, Any]) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO chunk_summaries (
                    chunk_id,
                    summary,
                    key_points,
                    entities,
                    category,
                    signal_score
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(chunk_id) DO UPDATE SET
                    summary = excluded.summary,
                    key_points = excluded.key_points,
                    entities = excluded.entities,
                    category = excluded.category,
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

    def upsert_video_summary(
        self, video_id: str, short_summary: str, detailed_summary: str
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO video_summaries (video_id, short_summary, detailed_summary)
                VALUES (?, ?, ?)
                ON CONFLICT(video_id) DO UPDATE SET
                    short_summary = excluded.short_summary,
                    detailed_summary = excluded.detailed_summary
                """,
                (video_id, short_summary, detailed_summary),
            )

    def video_exists(self, video_id: str) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM videos WHERE video_id = ?",
                (video_id,),
            ).fetchone()
        return row is not None

    def get_transcript(self, video_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT raw_text, cleaned_text
                FROM transcripts
                WHERE video_id = ?
                """,
                (video_id,),
            ).fetchone()
        return dict(row) if row else None

    def get_chunks(self, video_id: str) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT chunk_id, video_id, chunk_no, text
                FROM chunks
                WHERE video_id = ?
                ORDER BY chunk_no
                """,
                (video_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_chunk_summaries(self, video_id: str) -> dict[str, dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT cs.chunk_id, cs.summary, cs.key_points, cs.entities, cs.category, cs.signal_score
                FROM chunk_summaries cs
                INNER JOIN chunks c ON c.chunk_id = cs.chunk_id
                WHERE c.video_id = ?
                """,
                (video_id,),
            ).fetchall()

        results: dict[str, dict[str, Any]] = {}
        for row in rows:
            results[row["chunk_id"]] = {
                "summary": row["summary"] or "",
                "key_points": json.loads(row["key_points"] or "[]"),
                "entities": json.loads(row["entities"] or "[]"),
                "category": json.loads(row["category"] or "[]"),
                "signal_score": float(row["signal_score"] or 0.0),
            }
        return results

    def get_video_summary(self, video_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT short_summary, detailed_summary
                FROM video_summaries
                WHERE video_id = ?
                """,
                (video_id,),
            ).fetchone()
        return dict(row) if row else None

    def get_metadata_only_rows(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    v.video_id,
                    v.title,
                    v.channel,
                    v.published_at,
                    v.url,
                    COALESCE(v.description, '') AS description,
                    COALESCE(v.transcript_source, '') AS transcript_source,
                    COALESCE(v.transcript_length, 0) AS transcript_length,
                    COALESCE(t.raw_text, '') AS raw_text,
                    COALESCE(t.cleaned_text, '') AS cleaned_text
                FROM videos v
                LEFT JOIN transcripts t ON t.video_id = v.video_id
                WHERE COALESCE(t.cleaned_text, '') = ''
                ORDER BY v.published_at DESC, v.video_id
                """
            ).fetchall()
        return [dict(row) for row in rows]
