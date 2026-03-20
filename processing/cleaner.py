import re


COMMON_PHRASES = [
    "thanks for watching",
    "like and subscribe",
    "smash that like button",
    "subscribe to the channel",
    "don not forget to subscribe",
    "don't forget to subscribe",
    "this video is sponsored by",
    "check out our sponsor",
    "hit the bell icon",
    "turn on notifications",
    "follow us on social media",
    "follow me on social media",
    "link in the description",
    "links in the description",
    "full video in the description",
    "comment below",
    "leave a comment",
    "captions by",
    "subtitles by",
    "transcribed by",
]

NON_SPEECH_MARKERS = [
    "music",
    "applause",
    "laughter",
    "laughing",
    "cheering",
    "foreign",
    "silence",
    "background music",
    "intro music",
    "outro music",
]

TIMESTAMP_PATTERN = re.compile(
    r"(?:(?<=\s)|^)(?:\d{1,2}:)?\d{1,2}:\d{2}(?:\s*[-\u2013>]+\s*(?:\d{1,2}:)?\d{1,2}:\d{2})?(?=\s|$)"
)
WEBVTT_TIMECODE_PATTERN = re.compile(
    r"^\s*\d{2}:\d{2}:\d{2}[\.,]\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}[\.,]\d{3}(?:\s+\S+)*\s*$"
)
NON_SPEECH_PATTERN = re.compile(
    r"[\(\[]\s*(?:"
    + "|".join(re.escape(marker) for marker in NON_SPEECH_MARKERS)
    + r")(?:[^\]\)]*)[\)\]]",
    flags=re.IGNORECASE,
)
DESCRIPTIVE_NON_SPEECH_PATTERN = re.compile(
    r"[\(\[]\s*(?:[A-Za-z-]+\s+){0,3}(?:"
    + "|".join(re.escape(marker) for marker in NON_SPEECH_MARKERS)
    + r")(?:\s+[A-Za-z-]+){0,3}\s*[\)\]]",
    flags=re.IGNORECASE,
)
MUSICAL_NOTE_PATTERN = re.compile(r"[\u266a\u266b]+[^\u266a\u266b]*[\u266a\u266b]+")
LEADING_SPEAKER_PATTERN = re.compile(
    r"^\s*(?:[A-Z][A-Z0-9&.'-]{1,20}|[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})\s*:\s+"
)
LEADING_QUOTE_SPEAKER_PATTERN = re.compile(
    r"^\s*(?:>>+\s*|>\s+|-\s+)+(?:[A-Z][A-Za-z0-9&.'-]{1,20}\s*:\s+)?"
)
ROLE_SPEAKER_PATTERN = re.compile(
    r"^\s*(?:speaker|host|guest|narrator|reporter|anchor|moderator|voiceover)(?:\s+\d+)?(?:\s*\([^)]*\))?\s*:\s+",
    flags=re.IGNORECASE,
)
HANDLE_LINE_PATTERN = re.compile(r"^\s*(?:@[\w.]+|\#[\w-]+)(?:\s+(?:@[\w.]+|\#[\w-]+))*\s*$")
HASHTAG_PATTERN = re.compile(r"(?:(?<=\s)|^)\#[\w-]+(?=\s|$)")
METADATA_ONLY_LINE_PATTERN = re.compile(
    r"^\s*(?:WEBVTT|NOTE(?:\s+.*)?|Kind:\s*captions|Language:\s*[A-Za-z-]+|Region:\s*.*|Style:\s*.*)\s*$",
    flags=re.IGNORECASE,
)
BRACKETED_CENSOR_PATTERN = re.compile(r"[\(\[]\s*_+\s*[\)\]]")
CUE_IDENTIFIER_PATTERN = re.compile(r"^\s*\d+\s*$")


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


def _clean_line(line: str) -> str:
    cleaned = line.strip()
    if not cleaned:
        return ""
    if METADATA_ONLY_LINE_PATTERN.fullmatch(cleaned):
        return ""
    if WEBVTT_TIMECODE_PATTERN.fullmatch(cleaned):
        return ""
    if CUE_IDENTIFIER_PATTERN.fullmatch(cleaned):
        return ""

    cleaned = TIMESTAMP_PATTERN.sub(" ", cleaned)
    cleaned = NON_SPEECH_PATTERN.sub(" ", cleaned)
    cleaned = DESCRIPTIVE_NON_SPEECH_PATTERN.sub(" ", cleaned)
    cleaned = BRACKETED_CENSOR_PATTERN.sub(" ", cleaned)
    cleaned = MUSICAL_NOTE_PATTERN.sub(" ", cleaned)
    cleaned = LEADING_SPEAKER_PATTERN.sub("", cleaned)
    cleaned = LEADING_QUOTE_SPEAKER_PATTERN.sub("", cleaned)
    cleaned = ROLE_SPEAKER_PATTERN.sub("", cleaned)
    cleaned = HASHTAG_PATTERN.sub(" ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ->:")

    if HANDLE_LINE_PATTERN.fullmatch(cleaned):
        return ""

    lowered = cleaned.lower()
    for phrase in COMMON_PHRASES:
        lowered_phrase = phrase.lower()
        if lowered == lowered_phrase:
            return ""
        if lowered.startswith(lowered_phrase + " "):
            return ""
    return cleaned


def clean_text(text: str) -> str:
    if not text:
        return ""

    text = re.sub(r"https?://\S+|www\.\S+", " ", text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [_clean_line(line) for line in text.splitlines()]
    lines = _remove_duplicate_lines(lines)
    cleaned = "\n".join(lines)

    for phrase in COMMON_PHRASES:
        cleaned = re.sub(re.escape(phrase), " ", cleaned, flags=re.IGNORECASE)

    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()
