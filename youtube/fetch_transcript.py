import html
import json
import re
import xml.etree.ElementTree as ET
from typing import Any

import requests

from config import REQUEST_TIMEOUT


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)


def _get_watch_html(video_id: str) -> str:
    response = requests.get(
        f"https://www.youtube.com/watch?v={video_id}",
        headers={"User-Agent": USER_AGENT},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.text


def _extract_player_response(html_text: str) -> dict[str, Any]:
    markers = [
        "ytInitialPlayerResponse = ",
        "var ytInitialPlayerResponse = ",
    ]
    for marker in markers:
        start = html_text.find(marker)
        if start == -1:
            continue
        json_start = html_text.find("{", start)
        if json_start == -1:
            continue

        depth = 0
        in_string = False
        escape = False
        for index in range(json_start, len(html_text)):
            char = html_text[index]
            if escape:
                escape = False
                continue
            if char == "\\":
                escape = True
                continue
            if char == '"':
                in_string = not in_string
                continue
            if in_string:
                continue
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    return json.loads(html_text[json_start : index + 1])
    raise ValueError("Unable to extract player response.")


def _extract_description(player_response: dict[str, Any]) -> str:
    details = player_response.get("videoDetails", {})
    short_description = details.get("shortDescription", "")
    return html.unescape(short_description).strip()


def _fetch_caption_text(base_url: str) -> str:
    response = requests.get(
        base_url,
        headers={"User-Agent": USER_AGENT},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    root = ET.fromstring(response.text)
    parts: list[str] = []
    for node in root.findall(".//text"):
        content = "".join(node.itertext())
        content = html.unescape(content).replace("\n", " ").strip()
        if content:
            parts.append(content)
    return " ".join(parts).strip()


def _select_caption_track(caption_tracks: list[dict[str, Any]]) -> tuple[str | None, str]:
    manual_track = None
    auto_track = None

    for track in caption_tracks:
        kind = track.get("kind")
        language_code = track.get("languageCode", "")
        if kind == "asr":
            if auto_track is None or language_code.startswith("en"):
                auto_track = track
        else:
            if manual_track is None or language_code.startswith("en"):
                manual_track = track

    if manual_track:
        return manual_track.get("baseUrl"), "manual"
    if auto_track:
        return auto_track.get("baseUrl"), "auto"
    return None, "none"


def get_transcript(video_id: str) -> dict[str, str]:
    try:
        html_text = _get_watch_html(video_id)
        player_response = _extract_player_response(html_text)
        caption_tracks = (
            player_response.get("captions", {})
            .get("playerCaptionsTracklistRenderer", {})
            .get("captionTracks", [])
        )
        base_url, source = _select_caption_track(caption_tracks)
        if base_url:
            try:
                transcript = _fetch_caption_text(base_url)
                if transcript:
                    return {"text": transcript, "source": source}
            except (requests.RequestException, ET.ParseError):
                pass

        description = _extract_description(player_response)
        if description:
            return {"text": description, "source": "description"}
    except requests.RequestException:
        pass
    except (ValueError, ET.ParseError, json.JSONDecodeError):
        pass

    return {"text": "", "source": "none"}
