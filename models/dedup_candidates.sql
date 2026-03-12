-- dedup_candidates: sync-log filtered deduplication feed for sync.py remove_records.
--
-- Wraps dedup_candidates_bq_only and excludes entities already processed by a
-- successful remove_records run, as recorded in actionbuilder_sync.sync_log.
--
-- Why: TMC's incremental replication never captures hard deletes (removing an
-- entity from a campaign generates no updated_at change). BQ therefore shows
-- removed entities indefinitely. Without this filter, re-running remove_records
-- would retry all 8,921+ already-processed entities on every invocation.
--
-- dedup_ambiguous intentionally references dedup_candidates_bq_only (the unfiltered
-- version) — it uses the full known-dedup set to exclude entities from the human
-- review queue, regardless of sync log state.
--
-- To revert to BQ-only mode when replication is reliable:
--   1. Delete this file.
--   2. Rename dedup_candidates_bq_only.sql → dedup_candidates.sql.
--   3. Revert dedup_ambiguous.sql ref back to dedup_candidates.
--   The sync_log table can remain as a permanent audit log.

SELECT *
FROM {{ ref('dedup_candidates_bq_only') }}
WHERE NOT EXISTS (
  SELECT 1
  FROM `proj-tmc-mem-com`.actionbuilder_sync.sync_log sl
  WHERE sl.entity_interact_id = delete_interact_id
    AND sl.operation = 'remove_from_campaign'
    AND sl.status IN ('ok', '404')  -- both mean entity is absent from campaign
)
