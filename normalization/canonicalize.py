from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from processing.cleaner import clean_text
from db.repository import CanonicalItem


CLEANING_VERSION = "rule-based.v1"


def build_item_id(source_id: str, external_id: str | None, url: str) -> str:
    if external_id:
        return f"{source_id}:{external_id}"
    return f"{source_id}:{url}"


def _infer_body_kind(transcript_source: str, raw_text: str) -> str:
    normalized_source = (transcript_source or "").strip().lower()
    if not raw_text.strip():
        return "metadata_only"
    if normalized_source in {"manual", "auto", "cached"}:
        return "full_text"
    if normalized_source in {"description", "api_description"}:
        return "description_only"
    return "partial_text"


def _infer_evidence_strength(body_kind: str, content_status: str) -> str:
    if content_status != "available":
        return "none"
    if body_kind == "full_text":
        return "medium"
    if body_kind == "partial_text":
        return "weak"
    if body_kind == "description_only":
        return "weak"
    return "none"


def _infer_article_body_kind(fetch_kind: str, raw_text: str) -> str:
    normalized_kind = (fetch_kind or "").strip().lower()
    if not raw_text.strip():
        return "metadata_only"
    if normalized_kind in {"meta_description", "rss_summary"}:
        return "description_only"
    if normalized_kind in {"public_html", "authenticated_html", "jsonld_articlebody"}:
        return "full_text"
    return "partial_text"


def _external_id_from_url(url: str) -> str:
    parsed = urlparse(url)
    candidate = f"{parsed.netloc}{parsed.path}".strip("/")
    return candidate or url


def canonicalize_youtube_video(
    source_id: str,
    video: dict[str, Any],
    transcript_payload: dict[str, Any],
) -> CanonicalItem:
    raw_text = str(transcript_payload.get("text", "") or "")
    cleaned_text = clean_text(raw_text)
    has_content = bool(cleaned_text.strip())
    body_kind = _infer_body_kind(
        str(transcript_payload.get("source", "")),
        raw_text,
    )
    content_status = "available" if has_content else "unavailable"
    if not has_content:
        body_kind = "metadata_only" if not raw_text.strip() else body_kind
    return CanonicalItem(
        item_id=build_item_id(source_id, video.get("video_id"), str(video["url"])),
        source_id=source_id,
        source_type="youtube_video",
        external_id=video.get("video_id"),
        title=video.get("title"),
        author=video.get("channel"),
        published_at=video.get("published_at"),
        url=str(video["url"]),
        raw_text=raw_text,
        cleaned_text=cleaned_text,
        body_kind=body_kind,
        content_status=content_status,
        content_warning=video.get("content_warning"),
        retrieval_diagnostics=dict(transcript_payload.get("diagnostics", {}) or {}),
        language="en",
        trust_level=video.get("trust_level", "medium"),
        evidence_strength=_infer_evidence_strength(body_kind, content_status),
        cleaning_version=CLEANING_VERSION,
        cleaning_diagnostics={
            "cleaner": CLEANING_VERSION,
            "raw_length": len(raw_text),
            "cleaned_length": len(cleaned_text),
        },
    )


def canonicalize_candidate_article(
    *,
    source_id: str,
    source_name: str,
    title: str,
    url: str,
    published_at: str | None,
    category: str | None,
    retrieval_diagnostics: dict[str, Any] | None = None,
) -> CanonicalItem:
    diagnostics = dict(retrieval_diagnostics or {})
    diagnostics.setdefault("metadata_only_reason", "candidate_only_metadata")
    if category is not None:
        diagnostics["category"] = category
    external_id = _external_id_from_url(url)
    return CanonicalItem(
        item_id=build_item_id(source_id, external_id, url),
        source_id=source_id,
        source_type="article",
        external_id=external_id,
        title=title,
        author=source_name,
        published_at=published_at,
        url=url,
        raw_text="",
        cleaned_text="",
        body_kind="metadata_only",
        content_status="unavailable",
        content_warning="Metadata only: body retrieval disabled for candidate collection.",
        retrieval_diagnostics=diagnostics,
        language=None,
        trust_level=None,
        evidence_strength="none",
        cleaning_version=CLEANING_VERSION,
        cleaning_diagnostics={
            "cleaner": CLEANING_VERSION,
            "raw_length": 0,
            "cleaned_length": 0,
            "metadata_only": True,
        },
    )


def canonicalize_article_content(
    *,
    source_id: str,
    source_name: str,
    title: str,
    url: str,
    published_at: str | None,
    category: str | None,
    raw_text: str,
    fetch_kind: str,
    retrieval_diagnostics: dict[str, Any] | None = None,
    trust_level: str | None = None,
    language: str | None = None,
    content_warning: str | None = None,
) -> CanonicalItem:
    diagnostics = dict(retrieval_diagnostics or {})
    if category is not None:
        diagnostics["category"] = category
    diagnostics["metadata_only"] = False
    diagnostics.pop("metadata_only_reason", None)
    cleaned_text = clean_text(raw_text)
    body_kind = _infer_article_body_kind(fetch_kind, raw_text)
    content_status = "available" if cleaned_text.strip() else "unavailable"
    if content_status != "available":
        body_kind = "metadata_only"
    external_id = _external_id_from_url(url)
    return CanonicalItem(
        item_id=build_item_id(source_id, external_id, url),
        source_id=source_id,
        source_type="article",
        external_id=external_id,
        title=title,
        author=source_name,
        published_at=published_at,
        url=url,
        raw_text=raw_text,
        cleaned_text=cleaned_text,
        body_kind=body_kind,
        content_status=content_status,
        content_warning=content_warning,
        retrieval_diagnostics=diagnostics,
        language=language,
        trust_level=trust_level,
        evidence_strength=_infer_evidence_strength(body_kind, content_status),
        cleaning_version=CLEANING_VERSION,
        cleaning_diagnostics={
            "cleaner": CLEANING_VERSION,
            "raw_length": len(raw_text),
            "cleaned_length": len(cleaned_text),
            "fetch_kind": fetch_kind,
        },
    )
