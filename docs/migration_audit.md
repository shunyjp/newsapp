# Migration Audit And Conflict Rules

This document defines the operating rules for `python main.py migrate --backfill-items-from-videos`.

## Goals

- Keep the legacy `videos` tables and new `items` tables in parallel for now.
- Improve auditability before adding new providers or full unification.
- Never auto-resolve conflicts by overwriting higher-level item outputs.
- Treat `metadata_only` as a body characteristic, not as a failure mode.

## Audit Outputs

`migrate` supports:

- `--audit-file`: writes one JSON record per legacy video.
- `--summary-file`: writes aggregate JSON for counts and reason breakdowns.

Each audit record includes:

- `action`: `create`, `update`, `skip`, `conflict`, or `error`
- `body_kind`
- `evidence_strength`
- `warning_code`
- `warning_message`
- `conflict_types`
- `applied_resources`

## Conflict Model

Conflicts are evaluated per sub-resource, not as a blanket item-level overwrite decision.

Current sub-resources:

- `item_summary`
- `item_chunks`
- `item_chunk_summaries`

`item` core fields are still backfilled, but higher-level sub-resources are protected when they differ from legacy data.

## Conflict Conditions

A conflict is recorded when the destination item already has a protected sub-resource and the legacy payload differs.

Conflict codes:

- `migrate.conflict.item_summary_diff`
  - Existing `item_summaries.short_summary` or `item_summaries.detailed_summary` differs from the legacy video summary.
- `migrate.conflict.item_chunks_diff`
  - Existing `item_chunks.text` sequence differs from the legacy chunk text sequence.
- `migrate.conflict.item_chunk_summaries_diff`
  - Existing `item_chunk_summaries` payload differs from the legacy chunk-summary payload.

## Apply Behavior

When a conflict exists:

- No automatic overwrite is performed for the conflicting sub-resource.
- Non-conflicting sub-resources may still be backfilled in the same run.
- The audit record stays `action=conflict`.
- `applied_resources` shows which sub-resources were still written safely.

Examples:

- Summary conflict only:
  - Core item fields may update.
  - Chunks may backfill if they do not conflict.
  - Summary is preserved and not overwritten.
- Chunk conflict only:
  - Core item fields may update.
  - Summary may backfill if it does not conflict.
  - Chunks and dependent chunk summaries are preserved and not overwritten.

## Dry-Run Behavior

`--dry-run` performs the same conflict classification and writes the same audit/summary structure, but does not write to SQLite.

## Summary JSON Requirements

The summary JSON includes:

- top-level counts such as `scanned`, `created`, `updated`, `skipped_existing`, `conflicts`, `warning_count`, `error_count`
- `action_counts`
- `conflict_type_counts`
- `body_kind_counts`
- `evidence_strength_counts`
- `warning_code_counts`

This is the contract to rely on for operations dashboards and manual review workflows.
