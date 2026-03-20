from config import CHUNK_SIZE, OVERLAP_RATIO


def split_into_chunks(
    text: str,
    chunk_size: int = CHUNK_SIZE,
    overlap_ratio: float = OVERLAP_RATIO,
) -> list[str]:
    normalized = " ".join(text.split())
    if not normalized:
        return []

    if len(normalized) <= chunk_size:
        return [normalized]

    overlap = max(1, int(chunk_size * overlap_ratio))
    chunks: list[str] = []
    start = 0
    text_length = len(normalized)

    while start < text_length:
        target_end = min(text_length, start + chunk_size)
        if target_end < text_length:
            split_at = normalized.rfind(" ", start, target_end)
            if split_at == -1 or split_at <= start:
                split_at = target_end
        else:
            split_at = text_length

        chunk = normalized[start:split_at].strip()
        if chunk:
            chunks.append(chunk)

        if split_at >= text_length:
            break

        start = max(0, split_at - overlap)

    return chunks
