from __future__ import annotations

import re


EXPLICIT_NOISE_TITLE_PATTERNS = (
    re.compile(r"^\s*\[?PR\]?\s*[:：\-\]]?", re.IGNORECASE),
    re.compile(r"^\s*【PR】", re.IGNORECASE),
    re.compile(r"^\s*Advertorial[:：\-\s]", re.IGNORECASE),
    re.compile(r"^\s*Sponsored[:：\-\s]", re.IGNORECASE),
)


def is_explicit_noise_title(title: str | None) -> bool:
    normalized = str(title or "")
    if normalized.strip().lower() == "skip to main content":
        return True
    if normalized.strip().lower() == "view all":
        return True
    return any(pattern.match(normalized) for pattern in EXPLICIT_NOISE_TITLE_PATTERNS)
