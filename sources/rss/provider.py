from __future__ import annotations

import json
from email.utils import parsedate_to_datetime
import re
from typing import Any, Callable
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen
from xml.etree import ElementTree
import html

from normalization.noise_rules import is_explicit_noise_title
from normalization.canonicalize import (
    canonicalize_article_content,
    canonicalize_candidate_article,
)
from sources.base import CollectRequest, SourceProvider


def _strip_namespace(tag: str) -> str:
    return tag.split("}", 1)[-1]


def _child_text(element: ElementTree.Element, *names: str) -> str | None:
    name_set = set(names)
    for child in list(element):
        if _strip_namespace(child.tag) in name_set:
            text = (child.text or "").strip()
            if text:
                return text
    return None


def _entry_link(element: ElementTree.Element) -> str | None:
    direct_link = _child_text(element, "link")
    if direct_link:
        return direct_link
    for child in list(element):
        if _strip_namespace(child.tag) != "link":
            continue
        href = (child.attrib.get("href") or "").strip()
        if href:
            return href
        text = (child.text or "").strip()
        if text:
            return text
    return None


def _entry_category(element: ElementTree.Element, fallback: str | None) -> str | None:
    category = _child_text(element, "category")
    if category:
        return category
    for child in list(element):
        if _strip_namespace(child.tag) != "category":
            continue
        term = (child.attrib.get("term") or "").strip()
        if term:
            return term
    return fallback


def _published_at(element: ElementTree.Element) -> str | None:
    raw_value = _child_text(element, "pubDate", "published", "updated")
    if not raw_value:
        return None
    try:
        return parsedate_to_datetime(raw_value).isoformat()
    except (TypeError, ValueError, IndexError, OverflowError):
        return raw_value


def _parse_feed_entries(xml_text: str) -> list[dict[str, Any]]:
    root = ElementTree.fromstring(xml_text)
    entries: list[dict[str, Any]] = []
    for element in root.iter():
        tag = _strip_namespace(element.tag)
        if tag not in {"item", "entry"}:
            continue
        entries.append(
            {
                "title": _child_text(element, "title"),
                "url": _entry_link(element),
                "published_at": _published_at(element),
                "category": _entry_category(element, None),
            }
        )
    return entries


def _strip_html_tags(value: str) -> str:
    without_tags = re.sub(r"<[^>]+>", " ", value)
    normalized = html.unescape(without_tags)
    return re.sub(r"\s+", " ", normalized).strip()


def _extract_meta_content(html_text: str, *names: str) -> str | None:
    for name in names:
        pattern = re.compile(
            rf"<meta[^>]+(?:name|property)=['\"]{re.escape(name)}['\"][^>]+content=['\"](?P<value>.*?)['\"]",
            re.IGNORECASE | re.DOTALL,
        )
        match = pattern.search(html_text)
        if not match:
            continue
        value = _strip_html_tags(match.group("value"))
        if value:
            return value
    return None


def _extract_jsonld_article_body(html_text: str) -> str | None:
    for match in re.finditer(
        r"<script[^>]+type=['\"]application/ld\+json['\"][^>]*>(?P<body>.*?)</script>",
        html_text,
        re.IGNORECASE | re.DOTALL,
    ):
        candidate = match.group("body").strip()
        if not candidate:
            continue
        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        bodies = _find_jsonld_article_body(payload)
        if bodies:
            return "\n\n".join(bodies)
    return None


def _find_jsonld_article_body(payload: Any) -> list[str]:
    bodies: list[str] = []
    if isinstance(payload, dict):
        article_body = payload.get("articleBody")
        if isinstance(article_body, str):
            text = _strip_html_tags(article_body)
            if text:
                bodies.append(text)
        graph = payload.get("@graph")
        if isinstance(graph, list):
            for entry in graph:
                bodies.extend(_find_jsonld_article_body(entry))
    elif isinstance(payload, list):
        for entry in payload:
            bodies.extend(_find_jsonld_article_body(entry))
    return bodies


def _extract_article_paragraphs(html_text: str) -> list[str]:
    sanitized = re.sub(r"<script[\s\S]*?</script>", " ", html_text, flags=re.IGNORECASE)
    sanitized = re.sub(r"<style[\s\S]*?</style>", " ", sanitized, flags=re.IGNORECASE)
    sanitized = re.sub(r"<noscript[\s\S]*?</noscript>", " ", sanitized, flags=re.IGNORECASE)
    body_root = _extract_article_root(sanitized)
    paragraphs: list[str] = []
    for match in re.finditer(r"<p\b[^>]*>(?P<body>.*?)</p>", body_root, re.IGNORECASE | re.DOTALL):
        text = _strip_html_tags(match.group("body"))
        if len(text) < 40:
            continue
        if text in paragraphs:
            continue
        paragraphs.append(text)
    return paragraphs


def _extract_article_root(html_text: str) -> str:
    patterns = (
        r"<article\b[^>]*>(?P<body>.*?)</article>",
        r"<main\b[^>]*>(?P<body>.*?)</main>",
        r"<div\b[^>]+class=['\"][^'\"]*(?:article|content|body|post-body|entry-content|blog-posts|article-body)[^'\"]*['\"][^>]*>(?P<body>.*?)</div>",
    )
    for pattern in patterns:
        match = re.search(pattern, html_text, re.IGNORECASE | re.DOTALL)
        if match:
            return match.group("body")
    return html_text


def _extract_article_text(html_text: str) -> tuple[str, str]:
    jsonld_body = _extract_jsonld_article_body(html_text)
    if jsonld_body:
        return jsonld_body, "jsonld_articlebody"
    paragraphs = _extract_article_paragraphs(html_text)
    if paragraphs:
        return "\n\n".join(paragraphs), "public_html"
    description = _extract_meta_content(html_text, "description", "og:description", "twitter:description")
    if description:
        return description, "meta_description"
    return "", "metadata_only"


def _compile_entry_url_patterns(source_config: dict[str, Any]) -> list[re.Pattern[str]]:
    patterns: list[re.Pattern[str]] = []
    for value in list(source_config.get("entry_url_patterns", []) or []):
        text = str(value).strip()
        if not text:
            continue
        patterns.append(re.compile(text))
    return patterns


def _compile_excluded_entry_url_patterns(source_config: dict[str, Any]) -> list[re.Pattern[str]]:
    patterns: list[re.Pattern[str]] = []
    for value in list(source_config.get("exclude_entry_url_patterns", []) or []):
        text = str(value).strip()
        if not text:
            continue
        patterns.append(re.compile(text))
    return patterns


def _compile_included_title_patterns(source_config: dict[str, Any]) -> list[re.Pattern[str]]:
    patterns: list[re.Pattern[str]] = []
    for value in list(source_config.get("include_title_patterns", []) or []):
        text = str(value).strip()
        if not text:
            continue
        patterns.append(re.compile(text, re.IGNORECASE))
    return patterns


def _url_matches_entry_patterns(url: str, patterns: list[re.Pattern[str]]) -> bool:
    if not patterns:
        return True
    return any(pattern.search(url) for pattern in patterns)


def _url_matches_excluded_patterns(url: str, patterns: list[re.Pattern[str]]) -> bool:
    return any(pattern.search(url) for pattern in patterns)


def _title_matches_included_patterns(title: str, patterns: list[re.Pattern[str]]) -> bool:
    if not patterns:
        return True
    return any(pattern.search(title) for pattern in patterns)


def _parse_html_anchor_entries(
    html_text: str,
    *,
    base_url: str,
    allowed_host: str,
    fallback_category: str | None,
    entry_url_patterns: list[re.Pattern[str]] | None = None,
    exclude_entry_url_patterns: list[re.Pattern[str]] | None = None,
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    compiled_patterns = list(entry_url_patterns or [])
    excluded_patterns = list(exclude_entry_url_patterns or [])
    normalized_base_url = base_url.rstrip("/")
    anchor_pattern = re.compile(
        r"<a\b[^>]*href=[\"'](?P<href>[^\"']+)[\"'][^>]*>(?P<label>.*?)</a>",
        re.IGNORECASE | re.DOTALL,
    )
    for match in anchor_pattern.finditer(html_text):
        href = urljoin(base_url, match.group("href").strip())
        parsed = urlparse(href)
        if allowed_host and allowed_host not in parsed.netloc:
            continue
        if not parsed.scheme.startswith("http"):
            continue
        if parsed.fragment:
            continue
        if href.rstrip("/") == normalized_base_url:
            continue
        if _url_matches_excluded_patterns(href, excluded_patterns):
            continue
        if not _url_matches_entry_patterns(href, compiled_patterns):
            continue
        if href in seen_urls:
            continue
        title = _strip_html_tags(match.group("label"))
        if not title or len(title) < 8:
            continue
        seen_urls.add(href)
        entries.append(
            {
                "title": title,
                "url": href,
                "published_at": None,
                "category": fallback_category,
            }
        )
    return entries


class RssCandidateSourceProvider(SourceProvider):
    source_type = "article"

    def __init__(
        self,
        source_config: dict[str, Any],
        *,
        http_get: Callable[[str], Any] | None = None,
        auth_fetch: Callable[[str], dict[str, Any] | None] | None = None,
    ) -> None:
        self.source_config = dict(source_config)
        self.source_id = str(source_config["source_id"])
        self.http_get = http_get or self._default_http_get
        self.auth_fetch = auth_fetch or self._default_auth_fetch
        self.entry_url_patterns = _compile_entry_url_patterns(self.source_config)
        self.excluded_entry_url_patterns = _compile_excluded_entry_url_patterns(self.source_config)
        self.included_title_patterns = _compile_included_title_patterns(self.source_config)

    def _default_http_get(self, url: str) -> Any:
        request = Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; newsfeed1/0.1; +https://example.invalid)",
                "Accept": "application/rss+xml, application/xml, text/xml, text/html;q=0.9, */*;q=0.8",
            },
        )
        with urlopen(request, timeout=30) as response:
            class SimpleResponse:
                def __init__(self, body: str) -> None:
                    self.text = body

                def raise_for_status(self) -> None:
                    return None

            return SimpleResponse(response.read().decode("utf-8", errors="replace"))

    def _feed_url(self) -> str:
        return str(self.source_config.get("feed_url") or self.source_config.get("scrape_url") or "")

    def _scrape_url(self) -> str | None:
        value = self.source_config.get("scrape_url")
        return str(value) if value else None

    def _default_category(self) -> str | None:
        value = self.source_config.get("category")
        return str(value) if value else None

    def _allowed_host(self) -> str:
        base_url = str(self.source_config.get("base_url", "") or "")
        return urlparse(base_url).netloc

    def _matches_entry_url(self, url: str) -> bool:
        if _url_matches_excluded_patterns(url, self.excluded_entry_url_patterns):
            return False
        return _url_matches_entry_patterns(url, self.entry_url_patterns)

    def _matches_entry_title(self, title: str) -> bool:
        return _title_matches_included_patterns(title, self.included_title_patterns)

    def _build_diagnostics(self, *, category: str | None) -> dict[str, Any]:
        diagnostics: dict[str, Any] = {
            "candidate_only": True,
            "candidate_mode": str(self.source_config.get("candidate_mode", "metadata_only")),
            "feed_url": self._feed_url(),
            "metadata_only": True,
            "body_fetch_attempted": False,
        }
        if category:
            diagnostics["category"] = category
        return diagnostics

    def _trust_level(self) -> str | None:
        value = self.source_config.get("trust_level")
        return str(value) if value else None

    def _language(self) -> str | None:
        value = self.source_config.get("language")
        if value:
            return str(value)
        if self.source_id.startswith("nikkei."):
            return "ja"
        return "en"

    def _fetch_article_content(self, url: str) -> tuple[str, str]:
        response = self.http_get(url)
        response.raise_for_status()
        return _extract_article_text(str(response.text))

    def _default_auth_fetch(self, url: str) -> dict[str, Any] | None:
        if "nikkei" not in self.source_id:
            return None
        try:
            from sources.rss.nikkei_playwright_auth import fetch_authenticated_article_body
        except ModuleNotFoundError:
            return None
        payload = fetch_authenticated_article_body(url)
        return payload if isinstance(payload, dict) else None

    def _load_candidate_entries(self, request: CollectRequest) -> tuple[list[dict[str, Any]], str]:
        last_error: Exception | None = None
        try:
            response = self.http_get(self._feed_url())
            response.raise_for_status()
            entries = [
                entry
                for entry in _parse_feed_entries(response.text)
                if self._matches_entry_url(str(entry.get("url", "") or ""))
            ]
            if entries:
                return entries, "rss"
        except Exception as exc:
            last_error = exc
        scrape_url = self._scrape_url()
        if not scrape_url:
            if last_error is not None:
                raise last_error
            raise ValueError(f"No candidate entries found for source '{self.source_id}'.")
        response = self.http_get(scrape_url)
        response.raise_for_status()
        entries = _parse_html_anchor_entries(
            response.text,
            base_url=scrape_url,
            allowed_host=self._allowed_host(),
            fallback_category=self._default_category(),
            entry_url_patterns=self.entry_url_patterns,
            exclude_entry_url_patterns=self.excluded_entry_url_patterns,
        )
        return entries, "html_fallback"

    def collect(self, request: CollectRequest) -> list[dict[str, Any]]:
        entries, fetch_mode = self._load_candidate_entries(request)
        items: list[dict[str, Any]] = []
        for entry in entries:
            title = entry.get("title")
            url = entry.get("url")
            if not title or not url:
                continue
            if not self._matches_entry_title(str(title)):
                continue
            if is_explicit_noise_title(str(title)):
                continue
            category = entry.get("category") or self._default_category()
            diagnostics = self._build_diagnostics(
                category=(str(category) if category is not None else None)
            )
            diagnostics["candidate_fetch_mode"] = fetch_mode
            item = self._canonicalize_entry(
                title=str(title),
                url=str(url),
                published_at=(
                    str(entry["published_at"])
                    if entry.get("published_at") is not None
                    else None
                ),
                category=(str(category) if category is not None else None),
                diagnostics=diagnostics,
            )
            items.append({"feed_entry": entry, "item": item})
            if len(items) >= request.max_items:
                break
        return items

    def _canonicalize_entry(
        self,
        *,
        title: str,
        url: str,
        published_at: str | None,
        category: str | None,
        diagnostics: dict[str, Any],
    ):
        diagnostics = dict(diagnostics)
        diagnostics["body_fetch_attempted"] = True
        try:
            raw_text, fetch_kind = self._fetch_article_content(url)
        except Exception as exc:
            diagnostics["failure_reason"] = f"body_fetch_failed:{exc.__class__.__name__}"
            raw_text = ""
            fetch_kind = "metadata_only"
        if raw_text.strip():
            diagnostics["body_fetch_mode"] = fetch_kind
            diagnostics["canonicalization_mode"] = fetch_kind
            return canonicalize_article_content(
                source_id=self.source_id,
                source_name=str(self.source_config.get("source_name", self.source_id)),
                title=title,
                url=url,
                published_at=published_at,
                category=category,
                raw_text=raw_text,
                fetch_kind=fetch_kind,
                retrieval_diagnostics=diagnostics,
                trust_level=self._trust_level(),
                language=self._language(),
            )
        diagnostics["metadata_only"] = True
        diagnostics["canonicalization_mode"] = "metadata_only"
        return canonicalize_candidate_article(
            source_id=self.source_id,
            source_name=str(self.source_config.get("source_name", self.source_id)),
            title=title,
            url=url,
            published_at=published_at,
            category=category,
            retrieval_diagnostics=diagnostics,
        )


class NikkeiXTechCandidateProvider(RssCandidateSourceProvider):
    def _load_candidate_entries(self, request: CollectRequest) -> tuple[list[dict[str, Any]], str]:
        entries, fetch_mode = super()._load_candidate_entries(request)
        filtered_entries = [
            entry for entry in entries
            if "/atcl/" in str(entry.get("url", "")) or "/article/" in str(entry.get("url", ""))
        ]
        return filtered_entries, fetch_mode

    def _build_diagnostics(self, *, category: str | None) -> dict[str, Any]:
        diagnostics = super()._build_diagnostics(category=category)
        diagnostics["provider"] = "nikkei_xtech_candidate"
        diagnostics["canonicalization_mode"] = "metadata_only"
        return diagnostics

    def _canonicalize_entry(
        self,
        *,
        title: str,
        url: str,
        published_at: str | None,
        category: str | None,
        diagnostics: dict[str, Any],
    ):
        diagnostics = dict(diagnostics)
        diagnostics["body_fetch_attempted"] = True
        try:
            raw_text, fetch_kind = self._fetch_article_content(url)
        except Exception as exc:
            diagnostics["failure_reason"] = f"body_fetch_failed:{exc.__class__.__name__}"
            raw_text = ""
            fetch_kind = "metadata_only"
        if not raw_text.strip():
            diagnostics["authenticated_fetch_attempted"] = True
            auth_payload = self.auth_fetch(url)
            if auth_payload is None:
                diagnostics["authenticated_fetch"] = False
                diagnostics.setdefault("authenticated_fetch_reason", "bridge_unavailable")
            else:
                diagnostics["authenticated_fetch"] = bool(auth_payload.get("authenticatedFetch"))
                auth_reason = str(auth_payload.get("reason", "") or "").strip()
                if auth_reason:
                    diagnostics["authenticated_fetch_reason"] = auth_reason
                auth_status_code = auth_payload.get("statusCode")
                if auth_status_code is not None:
                    diagnostics["authenticated_fetch_status_code"] = auth_status_code
            auth_text = str((auth_payload or {}).get("text", "") or "").strip()
            if auth_text:
                raw_text = auth_text
                fetch_kind = "authenticated_html"
                diagnostics["body_fetch_mode"] = fetch_kind
                diagnostics["authenticated_fetch"] = True
        if raw_text.strip():
            diagnostics["body_fetch_mode"] = fetch_kind
            diagnostics["canonicalization_mode"] = fetch_kind
            return canonicalize_article_content(
                source_id=self.source_id,
                source_name=str(self.source_config.get("source_name", self.source_id)),
                title=title,
                url=url,
                published_at=published_at,
                category=category,
                raw_text=raw_text,
                fetch_kind=fetch_kind,
                retrieval_diagnostics=diagnostics,
                trust_level=self._trust_level(),
                language=self._language(),
            )
        diagnostics["metadata_only"] = True
        diagnostics.setdefault("metadata_only_reason", "candidate_only_metadata")
        diagnostics["canonicalization_mode"] = "metadata_only"
        return canonicalize_candidate_article(
            source_id=self.source_id,
            source_name=str(self.source_config.get("source_name", self.source_id)),
            title=title,
            url=url,
            published_at=published_at,
            category=category,
            retrieval_diagnostics=diagnostics,
        )
