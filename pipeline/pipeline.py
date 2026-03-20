from concurrent.futures import ThreadPoolExecutor
from typing import Any

from config import (
    CHUNK_SIZE,
    CHUNK_WORKERS,
    DB_PATH,
    OLLAMA_MODEL,
    OVERLAP_RATIO,
    VIDEO_WORKERS,
)
from db.database import Database
from llm.summarizer import DEFAULT_SUMMARY, summarize_chunk
from processing.chunker import split_into_chunks
from processing.cleaner import clean_text
from youtube.fetch_transcript import get_transcript
from youtube.fetch_videos import fetch_videos


CONTENT_UNAVAILABLE_SUMMARY = "Content unavailable: transcript and description could not be retrieved."
CONTENT_UNAVAILABLE_DETAIL = (
    "This video was stored with metadata only because neither transcript nor usable description was available."
)


class NewsPipeline:
    def __init__(
        self,
        db: Database,
        model: str = OLLAMA_MODEL,
        video_workers: int = VIDEO_WORKERS,
        chunk_workers: int = CHUNK_WORKERS,
        skip_llm: bool = False,
        resume_only_missing: bool = False,
        skip_existing_videos: bool = False,
    ) -> None:
        self.db = db
        self.model = model
        self.video_workers = max(1, video_workers)
        self.chunk_workers = max(1, chunk_workers)
        self.skip_llm = skip_llm
        self.resume_only_missing = resume_only_missing
        self.skip_existing_videos = skip_existing_videos

    def run(
        self,
        query: str | None = None,
        channel_id: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        videos = fetch_videos(query=query, channel_id=channel_id, limit=limit or 5)
        if self.skip_existing_videos:
            videos = [
                video for video in videos if not self.db.video_exists(video["video_id"])
            ]
        if not videos:
            return []
        max_workers = min(self.video_workers, max(1, len(videos)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(self._process_video, videos))
        return results

    def _process_video(self, video: dict[str, Any]) -> dict[str, Any]:
        self.db.upsert_video(video)
        transcript_payload, cleaned_text = self._resolve_transcript(video["video_id"])
        if (
            transcript_payload["source"] == "none"
            and not cleaned_text
            and video.get("description", "").strip()
        ):
            transcript_payload = {
                "text": video["description"].strip(),
                "source": "api_description",
            }
            cleaned_text = clean_text(transcript_payload["text"])
            self.db.upsert_transcript(video["video_id"], transcript_payload["text"], cleaned_text)
            self.db.update_video_transcript_metadata(
                video["video_id"],
                transcript_payload["source"],
                len(transcript_payload["text"]),
            )

        has_content = bool(cleaned_text.strip())
        stored_chunks, reused_chunks = self._resolve_chunks(video["video_id"], cleaned_text)
        chunk_summaries, reused_all_chunk_summaries = self._resolve_chunk_summaries(
            video["video_id"],
            stored_chunks,
        )
        aggregated = self._resolve_video_summary(
            video["video_id"],
            chunk_summaries,
            has_content=has_content,
            reused_chunks=reused_chunks,
            reused_all_chunk_summaries=reused_all_chunk_summaries,
        )
        aggregate_metadata = self._aggregate_metadata(chunk_summaries)
        if not has_content:
            aggregate_metadata = {
                "key_points": [],
                "entities": [],
                "categories": [],
                "why_it_matters": CONTENT_UNAVAILABLE_SUMMARY,
                "signal_score": 0.0,
            }
        chunk_summary_rows = self._build_chunk_summary_rows(stored_chunks, chunk_summaries)

        return {
            "video_id": video["video_id"],
            "title": video["title"],
            "channel": video.get("channel", ""),
            "published_at": video.get("published_at", ""),
            "url": video["url"],
            "transcript_source": transcript_payload["source"],
            "transcript_length": len(transcript_payload.get("text", "")),
            "content_status": "available" if has_content else "unavailable",
            "content_warning": "" if has_content else CONTENT_UNAVAILABLE_SUMMARY,
            "cleaned_text": cleaned_text,
            "short_summary": aggregated["short_summary"],
            "detailed_summary": aggregated["detailed_summary"],
            "reader_points": aggregate_metadata["key_points"][:5],
            "why_it_matters": aggregate_metadata["why_it_matters"],
            "signal_score": aggregate_metadata["signal_score"],
            "aggregated_key_points": aggregate_metadata["key_points"],
            "aggregated_entities": aggregate_metadata["entities"],
            "aggregated_categories": aggregate_metadata["categories"],
            "chunk_summaries": chunk_summary_rows,
        }

    def _resolve_transcript(
        self, video_id: str
    ) -> tuple[dict[str, str], str]:
        existing = self.db.get_transcript(video_id) if self.resume_only_missing else None
        if existing and existing.get("cleaned_text"):
            return {
                "text": existing.get("raw_text", ""),
                "source": "cached",
            }, existing.get("cleaned_text", "")

        transcript_payload = get_transcript(video_id)
        raw_text = transcript_payload["text"]
        cleaned_text = clean_text(raw_text)

        self.db.upsert_transcript(video_id, raw_text, cleaned_text)
        self.db.update_video_transcript_metadata(
            video_id,
            transcript_payload["source"],
            len(raw_text),
        )
        return transcript_payload, cleaned_text

    def _resolve_chunks(
        self, video_id: str, cleaned_text: str
    ) -> tuple[list[dict[str, Any]], bool]:
        existing_chunks = self.db.get_chunks(video_id) if self.resume_only_missing else []
        if existing_chunks:
            return existing_chunks, True

        chunks = split_into_chunks(
            cleaned_text,
            chunk_size=CHUNK_SIZE,
            overlap_ratio=OVERLAP_RATIO,
        )
        return self.db.replace_chunks(video_id, chunks), False

    def _resolve_chunk_summaries(
        self, video_id: str, stored_chunks: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], bool]:
        if self.skip_llm:
            return [], False

        existing = self.db.get_chunk_summaries(video_id) if self.resume_only_missing else {}
        missing_chunks = [
            record for record in stored_chunks if record["chunk_id"] not in existing
        ]

        new_summaries = self._summarize_chunks(missing_chunks)
        if not self.resume_only_missing:
            return new_summaries, False

        ordered_summaries: list[dict[str, Any]] = []
        new_summary_map = {
            record["chunk_id"]: summary
            for record, summary in zip(missing_chunks, new_summaries)
        }
        for chunk in stored_chunks:
            chunk_id = chunk["chunk_id"]
            if chunk_id in existing:
                ordered_summaries.append(existing[chunk_id])
            else:
                ordered_summaries.append(new_summary_map.get(chunk_id, dict(DEFAULT_SUMMARY)))
        return ordered_summaries, len(missing_chunks) == 0

    def _resolve_video_summary(
        self,
        video_id: str,
        chunk_summaries: list[dict[str, Any]],
        has_content: bool,
        reused_chunks: bool,
        reused_all_chunk_summaries: bool,
    ) -> dict[str, str]:
        existing = self.db.get_video_summary(video_id) if self.resume_only_missing else None
        if not has_content:
            summary = {
                "short_summary": CONTENT_UNAVAILABLE_SUMMARY,
                "detailed_summary": CONTENT_UNAVAILABLE_DETAIL,
            }
            self.db.upsert_video_summary(
                video_id,
                summary["short_summary"],
                summary["detailed_summary"],
            )
            return summary

        if existing and not chunk_summaries:
            return {
                "short_summary": existing.get("short_summary", "No summary available."),
                "detailed_summary": existing.get(
                    "detailed_summary", "No detailed summary available."
                ),
            }

        if self.skip_llm:
            summary = existing or {
                "short_summary": "LLM summarization skipped.",
                "detailed_summary": "LLM summarization skipped.",
            }
            self.db.upsert_video_summary(
                video_id,
                summary["short_summary"],
                summary["detailed_summary"],
            )
            return {
                "short_summary": summary["short_summary"],
                "detailed_summary": summary["detailed_summary"],
            }

        if (
            existing
            and self.resume_only_missing
            and reused_chunks
            and reused_all_chunk_summaries
        ):
            return {
                "short_summary": existing.get("short_summary", "No summary available."),
                "detailed_summary": existing.get(
                    "detailed_summary", "No detailed summary available."
                ),
            }

        aggregated = self._aggregate_video_summary(chunk_summaries)
        self.db.upsert_video_summary(
            video_id,
            aggregated["short_summary"],
            aggregated["detailed_summary"],
        )
        return aggregated

    def _aggregate_metadata(self, chunk_summaries: list[dict[str, Any]]) -> dict[str, Any]:
        key_points: list[str] = []
        entities: list[str] = []
        categories: list[str] = []
        seen_key_points: set[str] = set()
        seen_entities: set[str] = set()
        seen_categories: set[str] = set()
        scores: list[float] = []

        for summary in chunk_summaries:
            scores.append(float(summary.get("signal_score", 0.0)))
            for point in summary.get("key_points", []):
                normalized = point.strip().lower()
                if normalized and normalized not in seen_key_points:
                    seen_key_points.add(normalized)
                    key_points.append(point.strip())
            for entity in summary.get("entities", []):
                normalized = entity.strip().lower()
                if normalized and normalized not in seen_entities:
                    seen_entities.add(normalized)
                    entities.append(entity.strip())
            for category in summary.get("category", []):
                normalized = category.strip().lower()
                if normalized and normalized not in seen_categories:
                    seen_categories.add(normalized)
                    categories.append(category.strip())

        if key_points:
            why_it_matters = key_points[0]
        else:
            why_it_matters = "No high-signal points identified."

        signal_score = sum(scores) / len(scores) if scores else 0.0
        return {
            "key_points": key_points,
            "entities": entities,
            "categories": categories,
            "why_it_matters": why_it_matters,
            "signal_score": signal_score,
        }

    def _build_chunk_summary_rows(
        self,
        stored_chunks: list[dict[str, Any]],
        chunk_summaries: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for chunk, summary in zip(stored_chunks, chunk_summaries):
            rows.append(
                {
                    "chunk_id": chunk["chunk_id"],
                    "chunk_no": chunk["chunk_no"],
                    "text": chunk["text"],
                    "summary": summary.get("summary", ""),
                    "key_points": summary.get("key_points", []),
                    "entities": summary.get("entities", []),
                    "category": summary.get("category", []),
                    "signal_score": summary.get("signal_score", 0.0),
                }
            )
        return rows

    def _summarize_chunks(self, stored_chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not stored_chunks:
            return []

        max_workers = min(self.chunk_workers, max(1, len(stored_chunks)))
        if max_workers == 1:
            summaries = [self._summarize_chunk_record(record) for record in stored_chunks]
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                summaries = list(executor.map(self._summarize_chunk_record, stored_chunks))

        for chunk_id, summary in summaries:
            self.db.upsert_chunk_summary(chunk_id, summary)

        return [summary for _, summary in summaries]

    def _summarize_chunk_record(
        self, chunk_record: dict[str, Any]
    ) -> tuple[str, dict[str, Any]]:
        try:
            summary = summarize_chunk(chunk_record["text"], model=self.model)
        except Exception:
            summary = dict(DEFAULT_SUMMARY)
        return chunk_record["chunk_id"], summary

    def _aggregate_video_summary(
        self, chunk_summaries: list[dict[str, Any]]
    ) -> dict[str, str]:
        key_points: list[str] = []
        seen: set[str] = set()

        ordered_summaries = sorted(
            chunk_summaries,
            key=lambda item: item.get("signal_score", 0.0),
            reverse=True,
        )

        for summary in ordered_summaries:
            for point in summary.get("key_points", []):
                normalized = point.strip().lower()
                if not normalized or normalized in seen:
                    continue
                seen.add(normalized)
                key_points.append(point.strip())

        if not key_points:
            for summary in ordered_summaries:
                fallback = summary.get("summary", "").strip()
                if fallback:
                    normalized = fallback.lower()
                    if normalized not in seen:
                        seen.add(normalized)
                        key_points.append(fallback)

        top_points = key_points[:3]
        short_summary = " | ".join(top_points) if top_points else "No summary available."
        detailed_summary = "\n".join(f"- {point}" for point in key_points)
        if not detailed_summary:
            detailed_summary = "No detailed summary available."

        return {
            "short_summary": short_summary,
            "detailed_summary": detailed_summary,
        }


def build_default_pipeline(
    video_workers: int = VIDEO_WORKERS,
    chunk_workers: int = CHUNK_WORKERS,
    skip_llm: bool = False,
    resume_only_missing: bool = False,
    skip_existing_videos: bool = False,
) -> NewsPipeline:
    db = Database(db_path=DB_PATH, schema_path="db/schema.sql")
    return NewsPipeline(
        db=db,
        video_workers=video_workers,
        chunk_workers=chunk_workers,
        skip_llm=skip_llm,
        resume_only_missing=resume_only_missing,
        skip_existing_videos=skip_existing_videos,
    )
