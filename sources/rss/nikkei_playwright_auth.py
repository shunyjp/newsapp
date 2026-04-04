from __future__ import annotations

import json
import os
import re
from html import unescape
from pathlib import Path
from typing import Any


AUTHENTICATED_DOMAIN_SETTINGS: dict[str, dict[str, Any]] = {
    "xtech.nikkei.com": {
        "label": "Nikkei xTECH",
        "cookie_env_key": "NIKKEI_XTECH_COOKIE",
    },
    "www.nikkei.com": {
        "label": "日本経済新聞",
        "cookie_env_key": "NIKKEI_COOKIE",
    },
    "nikkei.com": {
        "label": "日本経済新聞",
        "cookie_env_key": "NIKKEI_COOKIE",
    },
    "financial.nikkei.com": {
        "label": "NIKKEI Financial",
        "cookie_env_key": "NIKKEI_COOKIE",
    },
    "bizgate.nikkei.com": {
        "label": "NIKKEI BizGate",
        "cookie_env_key": "NIKKEI_COOKIE",
    },
}

DEFAULT_BROWSER_PATHS = [
    os.environ.get("PLAYWRIGHT_BROWSER_PATH", ""),
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
]

REPO_ROOT = Path(__file__).resolve().parents[2]
SESSION_DIR = REPO_ROOT / "data"
STORAGE_STATE_PATH = SESSION_DIR / "nikkei-storage-state.json"


def _find_domain_settings(url: str) -> tuple[str, dict[str, Any]] | None:
    for domain, settings in AUTHENTICATED_DOMAIN_SETTINGS.items():
        if domain in url:
            return domain, settings
    return None


def _get_browser_executable_path() -> str | None:
    for candidate in DEFAULT_BROWSER_PATHS:
        if candidate and Path(candidate).exists():
            return candidate
    return None


def _get_target_urls() -> list[str]:
    values = [
        os.environ.get("NIKKEI_LOGIN_URL", "https://www.nikkei.com/"),
        os.environ.get("NIKKEI_XTECH_LOGIN_URL", "https://xtech.nikkei.com/"),
    ]
    seen: set[str] = set()
    urls: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            urls.append(value)
    return urls


def _playwright_login_enabled() -> bool:
    value = str(os.environ.get("NIKKEI_ENABLE_PLAYWRIGHT_LOGIN", "") or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _has_credentials() -> bool:
    login_id = os.environ.get("NIKKEI_LOGIN_ID") or os.environ.get("NIKKEI_XTECH_LOGIN_ID") or ""
    password = (
        os.environ.get("NIKKEI_LOGIN_PASSWORD")
        or os.environ.get("NIKKEI_XTECH_LOGIN_PASSWORD")
        or ""
    )
    return bool(login_id and password)


def _login_selectors() -> dict[str, str]:
    return {
        "login_button": (
            os.environ.get("NIKKEI_LOGIN_BUTTON_SELECTOR")
            or os.environ.get("NIKKEI_XTECH_LOGIN_BUTTON_SELECTOR")
            or 'a[href*="login"], button[href*="login"], .login, .btn-login'
        ),
        "login_id": (
            os.environ.get("NIKKEI_LOGIN_ID_SELECTOR")
            or os.environ.get("NIKKEI_XTECH_LOGIN_ID_SELECTOR")
            or 'input[type="email"], input[name="mail"], input[name="userId"], input[name="loginId"], input[type="text"]'
        ),
        "password": (
            os.environ.get("NIKKEI_LOGIN_PASSWORD_SELECTOR")
            or os.environ.get("NIKKEI_XTECH_LOGIN_PASSWORD_SELECTOR")
            or 'input[type="password"], input[name="password"]'
        ),
        "submit": (
            os.environ.get("NIKKEI_LOGIN_SUBMIT_SELECTOR")
            or os.environ.get("NIKKEI_XTECH_LOGIN_SUBMIT_SELECTOR")
            or 'button[type="submit"], input[type="submit"], .btn-login'
        ),
        "success": (
            os.environ.get("NIKKEI_LOGIN_SUCCESS_SELECTOR")
            or os.environ.get("NIKKEI_XTECH_LOGIN_SUCCESS_SELECTOR")
            or 'a[href*="logout"], button[href*="logout"], .user, .account'
        ),
    }


def _load_storage_state() -> dict[str, Any] | None:
    if not STORAGE_STATE_PATH.exists():
        return None
    try:
        return json.loads(STORAGE_STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _save_storage_state(state: dict[str, Any]) -> None:
    SESSION_DIR.mkdir(parents=True, exist_ok=True)
    STORAGE_STATE_PATH.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _filter_relevant_cookies(cookies: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    relevant: list[dict[str, Any]] = []
    for cookie in list(cookies or []):
        domain = str(cookie.get("domain", "") or "")
        if "nikkei.com" in domain or "xtech.nikkei.com" in domain:
            relevant.append(cookie)
    return relevant


def _cookies_to_header(cookies: list[dict[str, Any]]) -> str:
    import time

    now = int(time.time())
    pairs: list[str] = []
    for cookie in cookies:
        expires = cookie.get("expires")
        if expires not in (None, "", -1):
            try:
                if int(float(expires)) <= now:
                    continue
            except (TypeError, ValueError):
                pass
        name = str(cookie.get("name", "") or "").strip()
        value = str(cookie.get("value", "") or "")
        if name:
            pairs.append(f"{name}={value}")
    return "; ".join(pairs)


def _get_saved_cookie_header() -> str:
    state = _load_storage_state()
    if not state:
        return ""
    cookies = _filter_relevant_cookies(list(state.get("cookies", []) or []))
    return _cookies_to_header(cookies)


def _build_payload(
    *,
    text: str = "",
    title: str = "",
    authenticated_fetch: bool = False,
    reason: str = "",
    status_code: int | None = None,
) -> dict[str, Any]:
    return {
        "text": text,
        "title": title,
        "authenticatedFetch": authenticated_fetch,
        "reason": reason,
        "statusCode": status_code,
    }


def _strip_tags(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", unescape(value or ""))).strip()


def _extract_meta(html_text: str, name: str) -> str:
    pattern = re.compile(
        rf"<meta[^>]+(?:property|name)=['\"]{re.escape(name)}['\"][^>]+content=['\"]([^'\"]+)['\"]",
        re.IGNORECASE,
    )
    match = pattern.search(html_text)
    return _strip_tags(match.group(1) if match else "")


def _extract_title(html_text: str) -> str:
    meta_title = _extract_meta(html_text, "og:title")
    if meta_title:
        return meta_title
    match = re.search(r"<title[^>]*>([\s\S]*?)</title>", html_text, re.IGNORECASE)
    return _strip_tags(match.group(1) if match else "")


def _extract_body_root(html_text: str) -> str:
    patterns = [
        re.compile(r"<article[^>]*>([\s\S]*?)</article>", re.IGNORECASE),
        re.compile(r"<main[^>]*>([\s\S]*?)</main>", re.IGNORECASE),
        re.compile(
            r"<div[^>]+class=['\"][^'\"]*(?:article|content|body|page-main|container_campx|cmn-article_body)[^'\"]*['\"][^>]*>([\s\S]*?)</div>",
            re.IGNORECASE,
        ),
    ]
    for pattern in patterns:
        match = pattern.search(html_text)
        if match:
            return match.group(1)
    return html_text


def _extract_paragraphs(html_text: str) -> list[str]:
    cleaned_html = re.sub(r"<script[\s\S]*?</script>", " ", html_text, flags=re.IGNORECASE)
    cleaned_html = re.sub(r"<style[\s\S]*?</style>", " ", cleaned_html, flags=re.IGNORECASE)
    cleaned_html = re.sub(r"<noscript[\s\S]*?</noscript>", " ", cleaned_html, flags=re.IGNORECASE)
    body = _extract_body_root(cleaned_html)
    paragraphs = [
        _strip_tags(match.group(1))
        for match in re.finditer(r"<p\b[^>]*>([\s\S]*?)</p>", body, flags=re.IGNORECASE)
    ]
    filtered: list[str] = []
    noise_pattern = re.compile(
        r"蛻ｩ逕ｨ隕冗ｴл莨壼藤逋ｻ骭ｲ|繝ｭ繧ｰ繧､繝ｳ|縺薙・險倅ｺ九・莨壼藤髯仙ｮ嘶蠎・相|縺薙・險倅ｺ九ｒ縺願ｪｭ縺ｿ縺・◆縺縺上↓縺ｯ",
        re.IGNORECASE,
    )
    for paragraph in paragraphs:
        if len(paragraph) <= 40:
            continue
        if noise_pattern.search(paragraph):
            continue
        filtered.append(paragraph)
        if len(filtered) >= 14:
            break
    return filtered


def login_to_nikkei_and_persist_session(force: bool = False) -> dict[str, Any]:
    if not _has_credentials():
        return {"ok": False, "reason": "missing_credentials"}

    if not force and _get_saved_cookie_header():
        return {"ok": True, "reused": True}

    try:
        from playwright.sync_api import sync_playwright
    except ModuleNotFoundError:
        return {"ok": False, "reason": "missing_playwright"}

    executable_path = _get_browser_executable_path()
    if not executable_path:
        return {"ok": False, "reason": "missing_browser"}

    selectors = _login_selectors()
    login_id = os.environ.get("NIKKEI_LOGIN_ID") or os.environ.get("NIKKEI_XTECH_LOGIN_ID") or ""
    password = (
        os.environ.get("NIKKEI_LOGIN_PASSWORD")
        or os.environ.get("NIKKEI_XTECH_LOGIN_PASSWORD")
        or ""
    )
    attempted_urls: list[str] = []

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=(
                    os.environ.get("NIKKEI_HEADLESS") != "false"
                    and os.environ.get("NIKKEI_XTECH_HEADLESS") != "false"
                ),
                executable_path=executable_path,
            )
            context = browser.new_context()
            page = context.new_page()
            try:
                for url in _get_target_urls():
                    attempted_urls.append(url)
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    login_button = page.locator(selectors["login_button"]).first
                    if login_button.count():
                        login_button.click(timeout=10000)
                    page.locator(selectors["login_id"]).first.fill(login_id, timeout=15000)
                    page.locator(selectors["password"]).first.fill(password, timeout=15000)
                    page.locator(selectors["submit"]).first.click(timeout=15000)
                    try:
                        page.wait_for_load_state("networkidle", timeout=60000)
                    except Exception:
                        pass
                    try:
                        page.locator(selectors["success"]).first.wait_for(timeout=15000)
                    except Exception:
                        pass
                _save_storage_state(context.storage_state())
                return {"ok": True, "reused": False, "attempted_urls": attempted_urls}
            except Exception as exc:
                return {
                    "ok": False,
                    "reason": f"playwright_login_failed:{exc.__class__.__name__}",
                    "attempted_urls": attempted_urls,
                }
            finally:
                browser.close()
    except Exception as exc:
        return {
            "ok": False,
            "reason": f"playwright_login_failed:{exc.__class__.__name__}",
            "attempted_urls": attempted_urls,
        }


def fetch_authenticated_article_body(url: str) -> dict[str, Any]:
    if not url:
        return _build_payload(reason="missing_url")

    matched = _find_domain_settings(url)
    if not matched:
        return _build_payload(reason="unsupported_domain")

    _domain, settings = matched
    cookie_header = os.environ.get(str(settings.get("cookie_env_key", "") or ""), "")
    storage_state_exists = STORAGE_STATE_PATH.exists()

    if not cookie_header and not _get_saved_cookie_header():
        if not _playwright_login_enabled():
            return _build_payload(reason="missing_cookie")
        login_result = login_to_nikkei_and_persist_session()
        if not login_result.get("ok"):
            return _build_payload(reason=str(login_result.get("reason", "missing_cookie")))
        storage_state_exists = STORAGE_STATE_PATH.exists()

    try:
        from playwright.sync_api import sync_playwright
    except ModuleNotFoundError:
        return _build_payload(reason="missing_playwright")

    executable_path = _get_browser_executable_path()
    if not executable_path:
        return _build_payload(reason="missing_browser")

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=(
                    os.environ.get("NIKKEI_HEADLESS") != "false"
                    and os.environ.get("NIKKEI_XTECH_HEADLESS") != "false"
                ),
                executable_path=executable_path,
            )
            context_kwargs: dict[str, Any] = {}
            if storage_state_exists:
                context_kwargs["storage_state"] = str(STORAGE_STATE_PATH)
            elif cookie_header:
                context_kwargs["extra_http_headers"] = {"Cookie": cookie_header}
            context = browser.new_context(**context_kwargs)
            page = context.new_page()
            try:
                response = page.goto(url, wait_until="domcontentloaded", timeout=60000)
                try:
                    page.wait_for_load_state("networkidle", timeout=15000)
                except Exception:
                    pass
                status_code = response.status if response is not None else None
                if response is not None and not response.ok:
                    return _build_payload(
                        reason=f"http_status:{response.status}",
                        status_code=response.status,
                    )
                html_text = page.content()
                title = _extract_title(html_text)
                paragraphs = _extract_paragraphs(html_text)
                if not paragraphs:
                    return _build_payload(
                        title=title,
                        reason="empty_paragraphs",
                        status_code=status_code,
                    )
                return _build_payload(
                    text=" ".join(paragraphs)[:4000],
                    title=title,
                    authenticated_fetch=True,
                    reason="success",
                    status_code=status_code,
                )
            finally:
                browser.close()
    except Exception as exc:
        return _build_payload(reason=f"playwright_fetch_failed:{exc.__class__.__name__}")
