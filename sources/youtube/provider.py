from __future__ import annotations

from typing import Any

from normalization.canonicalize import canonicalize_youtube_video
from sources.base import CollectRequest, SourceProvider
from youtube.fetch_transcript import get_transcript
from youtube.fetch_videos import fetch_videos


class YouTubeSourceProvider(SourceProvider):
    source_id = "youtube.default"
    source_type = "youtube_video"

    def collect(self, request: CollectRequest) -> list[dict[str, Any]]:
        videos = fetch_videos(
            query=request.query,
            channel_id=request.channel_id,
            limit=request.max_items,
        )
        items: list[dict[str, Any]] = []
        for video in videos:
            transcript_payload = get_transcript(video["video_id"])
            item = canonicalize_youtube_video(
                source_id=self.source_id,
                video=video,
                transcript_payload=transcript_payload,
            )
            items.append(
                {
                    "source_video": video,
                    "transcript_payload": transcript_payload,
                    "item": item,
                }
            )
        return items
