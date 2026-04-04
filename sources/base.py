from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class CollectRequest:
    source_id: str
    query: str | None = None
    channel_id: str | None = None
    max_items: int = 5


class SourceProvider(ABC):
    source_id: str
    source_type: str

    @abstractmethod
    def collect(self, request: CollectRequest) -> list[dict[str, Any]]:
        raise NotImplementedError


class SourceRegistry:
    def __init__(self) -> None:
        self._providers: dict[str, SourceProvider] = {}

    def register(self, provider: SourceProvider) -> None:
        self._providers[provider.source_id] = provider

    def get(self, source_id: str) -> SourceProvider:
        try:
            return self._providers[source_id]
        except KeyError as exc:
            available = ", ".join(sorted(self._providers))
            raise ValueError(
                f"Unknown source '{source_id}'. Available sources: {available}"
            ) from exc

    def list_source_ids(self) -> list[str]:
        return sorted(self._providers)
