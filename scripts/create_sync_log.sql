-- One-time setup: create the sync_log table in actionbuilder_sync.
--
-- Run this once from the BQ console or via `bq query --use_legacy_sql=false`.
-- The table is managed outside dbt (not in models/) so it persists across
-- dbt runs without being recreated or truncated.
--
-- Purpose: records every successful (and failed) sync.py API call so that
-- dbt views can filter already-processed records when BQ replication is lagging.
-- The two primary use cases:
--   - dedup_candidates: excludes entities already logged as remove_from_campaign
--   - deduplicated_names_to_load: excludes person_ids already logged as insert_entity
--
-- To revert to BQ-only mode when replication is reliable:
--   1. Delete models/dedup_candidates.sql (the sync-log wrapper)
--   2. Rename models/dedup_candidates_bq_only.sql → dedup_candidates.sql
--   3. Delete models/deduplicated_names_to_load.sql (the sync-log wrapper)
--   4. Rename models/deduplicated_names_to_load_bq_only.sql → deduplicated_names_to_load.sql
--   5. Revert models/dedup_ambiguous.sql ref back to dedup_candidates
--   The sync_log table can remain as a permanent audit log.

CREATE TABLE IF NOT EXISTS `proj-tmc-mem-com`.actionbuilder_sync.sync_log (

  -- Unique ID for a single script invocation (UUID4 generated at startup).
  -- All rows from one sync.py run share the same run_id.
  run_id               STRING NOT NULL,

  -- Operation that was performed. Values:
  --   'remove_from_campaign'  remove_records (delete_person API call)
  --   'insert_entity'         insert_new_records (insert_entity API call)
  --   'add_email'             prepare_email_data (update_person with email)
  --   'add_phone'             prepare_phone_data (update_person with phone)
  operation            STRING NOT NULL,

  -- AB entity UUID (interact_id, 36 chars).
  -- For remove_from_campaign: the entity being removed.
  -- For insert_entity: NULL (entity doesn't exist yet at call time).
  entity_interact_id   STRING,

  -- AB campaign UUID (interact_id, 36 chars).
  campaign_interact_id STRING,

  -- Source-system person_id from core_enhanced.
  -- Populated for insert_entity ops (used by deduplicated_names_to_load filter).
  -- NULL for all other operations.
  person_id            STRING,

  -- Tag/tagging UUIDs — used for tag-level logging (add_tagging, delete_tagging).
  tag_interact_id      STRING,
  tagging_interact_id  STRING,

  -- Tag metadata — used by the current_tag_values overlay to reconstruct
  -- tag state from sync_log when taggable_logbook replication is stale.
  --   tag_name:       human-readable tag name (e.g. "Events Attended Past 6 Months")
  --   value_written:  the value that was written (number as string, date, or 'applied')
  tag_name             STRING,
  value_written        STRING,

  -- Timestamp of the API call (CURRENT_TIMESTAMP at time of call).
  executed_at          TIMESTAMP NOT NULL,

  -- Result of the API call:
  --   'ok'     Success.
  --   '404'    Entity/resource already absent — desired state achieved.
  --   'error'  Unexpected error — see error_detail.
  status               STRING NOT NULL,

  -- Error message (truncated to 500 chars) when status='error'. NULL otherwise.
  error_detail         STRING

)
PARTITION BY DATE(executed_at)
OPTIONS (
  description = 'Audit log of all sync.py API calls. Used by dbt views (dedup_candidates, deduplicated_names_to_load) to filter already-processed records when AB->BQ replication is lagging. See sync_log_plan.md for full architecture.'
);
