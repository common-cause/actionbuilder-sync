-- deduplicated_names_to_load: sync-log filtered insertion feed for sync.py insert_new_records.
--
-- Wraps deduplicated_names_to_load_bq_only and excludes person_ids already
-- successfully inserted by a prior insert_new_records run, as recorded in
-- actionbuilder_sync.sync_log.
--
-- Why: BQ replication of newly-inserted entities can lag by hours. Without this
-- filter, re-running insert_new_records before BQ catches up would attempt to
-- insert the same people again (the BQ-only exclusion checks would not yet see
-- the new entities in actionbuilder_cleaned).
--
-- Matches on person_id (the source-system identity available in both this view
-- and the sync log). Records with NULL person_id pass through unchanged —
-- they cannot be matched by person_id and rely solely on BQ contact exclusion.
--
-- To revert to BQ-only mode when replication is reliable:
--   1. Delete this file.
--   2. Rename deduplicated_names_to_load_bq_only.sql → deduplicated_names_to_load.sql.
--   The sync_log table can remain as a permanent audit log.

SELECT *
FROM {{ ref('deduplicated_names_to_load_bq_only') }}
WHERE person_id IS NULL
   OR person_id NOT IN (
     SELECT person_id
     FROM `proj-tmc-mem-com`.actionbuilder_sync.sync_log
     WHERE operation = 'insert_entity'
       AND status = 'ok'
       AND person_id IS NOT NULL
   )
