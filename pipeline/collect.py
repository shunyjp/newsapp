from __future__ import annotations

from dataclasses import asdict
from typing import Any

from db.database import Database
from db.repository import ItemRepository, source_record_from_dict
from pipeline.source_config import load_source_config, resolve_source_ids, source_map
from sources.base import CollectRequest, SourceRegistry
from sources.rss.provider import NikkeiXTechCandidateProvider, RssCandidateSourceProvider
from sources.youtube.provider import YouTubeSourceProvider


def build_source_registry(config: dict[str, Any]) -> SourceRegistry:
    registry = SourceRegistry()
    registry.register(YouTubeSourceProvider())
    for source_id, source in source_map(config).items():
        provider_name = str(source.get("provider", "") or "")
        if source_id == "youtube.default":
            continue
        if provider_name == "nikkei_xtech_candidate":
            registry.register(NikkeiXTechCandidateProvider(source))
        elif provider_name == "rss_candidate":
            registry.register(RssCandidateSourceProvider(source))
    return registry


def sync_source_registry(repository: ItemRepository) -> dict[str, Any]:
    config = load_source_config()
    source_records = [source_record_from_dict(item) for item in config.get("sources", [])]
    repository.sync_sources(source_records)
    return config


def collect_items(
    db: Database,
    source_id: str | None,
    query: str | None,
    channel_id: str | None,
    max_items: int,
    source_set: str | None = None,
) -> list[dict[str, Any]]:
    repository = ItemRepository(db)
    config = sync_source_registry(repository)
    registry = build_source_registry(config)
    records: list[dict[str, Any]] = []
    source_ids = resolve_source_ids(config, source_id=source_id, source_set=source_set)
    for resolved_source_id in source_ids:
        provider = registry.get(resolved_source_id)
        request = CollectRequest(
            source_id=resolved_source_id,
            query=query,
            channel_id=channel_id,
            max_items=max_items,
        )
        provider_records = provider.collect(request)
        for record in provider_records:
            repository.upsert_item(record["item"])
            record["source_id"] = resolved_source_id
        records.extend(provider_records)
    return records


def serialize_collect_record(record: dict[str, Any]) -> dict[str, Any]:
    payload = dict(record)
    if payload.get("item") is not None:
        payload["item"] = asdict(payload["item"])
    return payload
