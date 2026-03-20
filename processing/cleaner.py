import re


COMMON_PHRASES = [
    "thanks for watching",
    "like and subscribe",
    "subscribe to the channel",
    "don not forget to subscribe",
    "don't forget to subscribe",
    "this video is sponsored by",
    "check out our sponsor",
    "hit the bell icon",
    "follow us on social media",
]


def _remove_duplicate_lines(lines: list[str]) -> list[str]:
    seen: set[str] = set()
    deduplicated: list[str] = []
    for line in lines:
        normalized = line.strip().lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduplicated.append(line.strip())
    return deduplicated


def clean_text(text: str) -> str:
    if not text:
        return ""

    text = re.sub(r"https?://\S+|www\.\S+", " ", text)
    lines = text.splitlines()
    lines = _remove_duplicate_lines(lines)
    cleaned = "\n".join(lines)

    for phrase in COMMON_PHRASES:
        cleaned = re.sub(re.escape(phrase), " ", cleaned, flags=re.IGNORECASE)

    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()
