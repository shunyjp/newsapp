from typing import Any

import requests

from config import MAX_VIDEOS, REQUEST_TIMEOUT, YOUTUBE_API_BASE_URL, YOUTUBE_API_KEY


def _get_api_key() -> str:
    if not YOUTUBE_API_KEY:
        raise ValueError(
            "YOUTUBE_API_KEY is not set. Define it in your environment before running the pipeline."
        )
    return YOUTUBE_API_KEY


def _api_get(endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
    response = requests.get(
        f"{YOUTUBE_API_BASE_URL}/{endpoint}",
        params={**params, "key": _get_api_key()},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def _fetch_video_details(video_ids: list[str]) -> list[dict[str, Any]]:
    if not video_ids:
        return []

    payload = _api_get(
        "videos",
        {
            "part": "snippet",
            "id": ",".join(video_ids),
            "maxResults": len(video_ids),
        },
    )
    items = payload.get("items", [])
    by_id: dict[str, dict[str, Any]] = {}
    for item in items:
        snippet = item.get("snippet", {})
        video_id = item.get("id", "")
        if not video_id:
            continue
        by_id[video_id] = {
            "video_id": video_id,
            "title": snippet.get("title", ""),
            "channel": snippet.get("channelTitle", ""),
            "published_at": snippet.get("publishedAt", ""),
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "description": snippet.get("description", ""),
        }
    return [by_id[video_id] for video_id in video_ids if video_id in by_id]


def _search_videos(query: str, limit: int) -> list[dict[str, Any]]:
    payload = _api_get(
        "search",
        {
            "part": "snippet",
            "q": query,
            "type": "video",
            "maxResults": min(limit, 50),
            "order": "date",
        },
    )
    items = payload.get("items", [])
    video_ids = [
        item.get("id", {}).get("videoId", "")
        for item in items
        if item.get("id", {}).get("videoId")
    ]
    videos = _fetch_video_details(video_ids[:limit])
    if not videos:
        raise ValueError("No videos found for the provided query.")
    return videos


def _fetch_upload_playlist_id(channel_id: str) -> str:
    payload = _api_get(
        "channels",
        {
            "part": "contentDetails",
            "id": channel_id,
            "maxResults": 1,
        },
    )
    items = payload.get("items", [])
    if not items:
        raise ValueError(f"No channel found for channel_id={channel_id}.")

    uploads_playlist_id = (
        items[0]
        .get("contentDetails", {})
        .get("relatedPlaylists", {})
        .get("uploads", "")
    )
    if not uploads_playlist_id:
        raise ValueError(f"Uploads playlist not found for channel_id={channel_id}.")
    return uploads_playlist_id


def _fetch_channel_videos(channel_id: str, limit: int) -> list[dict[str, Any]]:
    uploads_playlist_id = _fetch_upload_playlist_id(channel_id)
    payload = _api_get(
        "playlistItems",
        {
            "part": "snippet,contentDetails",
            "playlistId": uploads_playlist_id,
            "maxResults": min(limit, 50),
        },
    )
    items = payload.get("items", [])
    video_ids = [
        item.get("contentDetails", {}).get("videoId", "")
        for item in items
        if item.get("contentDetails", {}).get("videoId")
    ]
    videos = _fetch_video_details(video_ids[:limit])
    if not videos:
        raise ValueError("No videos found for the provided channel.")
    return videos


def fetch_videos(
    query: str | None = None,
    channel_id: str | None = None,
    limit: int = MAX_VIDEOS,
) -> list[dict[str, Any]]:
    if bool(query) == bool(channel_id):
        raise ValueError("Provide exactly one of query or channel_id.")

    limit = max(1, limit)

    if channel_id:
        return _fetch_channel_videos(channel_id, limit=limit)

    assert query is not None
    normalized_query = query.strip()
    return _search_videos(normalized_query, limit=limit)
