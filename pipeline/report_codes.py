from __future__ import annotations


MIGRATE_REASON_MESSAGES = {
    "migrate.skip.existing_item_only_missing": "Existing item was preserved because --only-missing was specified.",
    "migrate.conflict.item_summary_diff": "Existing item summary differs from the legacy summary, so the summary backfill was skipped.",
    "migrate.conflict.item_chunks_diff": "Existing item chunks differ from the legacy chunks, so chunk backfill was skipped.",
    "migrate.conflict.item_chunk_summaries_diff": "Existing chunk summaries differ from the legacy chunk summaries, so chunk-summary backfill was skipped.",
}

ANALYZE_REASON_MESSAGES = {
    "analyze.missing.contents_absent": "Both raw_text and cleaned_text are absent.",
    "analyze.missing.cleaned_text_empty": "cleaned_text is empty.",
    "analyze.missing.chunks_missing": "No item_chunks exist for the available cleaned_text.",
    "analyze.missing.summary_missing": "No item_summaries row exists.",
    "analyze.missing.quality_missing": "Quality fields have not been evaluated yet.",
    "analyze.retry.ineligible": "Item is currently marked ineligible by export policy.",
    "analyze.retry.low_quality": "Item is currently marked as low quality.",
}

EXPORT_REASON_MESSAGES = {
    "export.exclusion.content_status_excluded": "Excluded because the content_status is not allowed for this export target.",
    "export.exclusion.body_kind_excluded": "Excluded because the body_kind is not allowed for this export target.",
    "export.exclusion.quality_excluded": "Excluded because quality_tier is below the export threshold.",
    "export.exclusion.eligibility_excluded": "Excluded because the target-specific eligibility is ineligible.",
    "export.exclusion.conditional_body_kind": "Included with warning because the body_kind requires conditional handling.",
    "export.exclusion.body_kind=metadata_only": "Included with warning because the body_kind is metadata_only.",
    "export.exclusion.body_kind=description_only": "Included with warning because the body_kind is description_only.",
    "export.exclusion.quality_tier=low": "Included with warning because quality_tier is low.",
    "export.exclusion.quality_tier=reject": "Included with warning because quality_tier is reject.",
    "export.exclusion.content_unavailable": "Included with warning because the content is unavailable.",
}


def describe_reason(code: str) -> str:
    for table in (MIGRATE_REASON_MESSAGES, ANALYZE_REASON_MESSAGES, EXPORT_REASON_MESSAGES):
        if code in table:
            return table[code]
    return "No description registered for this code."
