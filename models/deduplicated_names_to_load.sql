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
-- and the sync log) AND on email (sync_log value_written for insert_entity rows,
-- logged since 2026-07-29). person_id alone is not enough: one email can carry
-- multiple core person_ids (upstream identity dupes), so night 2 saw the second
-- person_id as new while BQ replication lag blinded the email-based already-in-AB
-- check — 16 duplicate entity pairs on 2026-06-17/18. Records with NULL person_id
-- AND no email match still pass through and rely on BQ contact exclusion.
-- (Same-person-different-email dupes remain out of reach of both keys; only the
-- dedup_candidates flow catches those.)
--
-- To revert to BQ-only mode when replication is reliable:
--   1. Delete this file.
--   2. Rename deduplicated_names_to_load_bq_only.sql → deduplicated_names_to_load.sql.
--   The sync_log table can remain as a permanent audit log.

SELECT *
FROM {{ ref('deduplicated_names_to_load_bq_only') }}
WHERE (person_id IS NULL
   OR person_id NOT IN (
     SELECT person_id
     FROM `proj-tmc-mem-com`.actionbuilder_sync.sync_log
     WHERE operation = 'insert_entity'
       AND status = 'ok'
       AND person_id IS NOT NULL
   ))
  AND (email IS NULL
   OR LOWER(TRIM(email)) NOT IN (
     SELECT LOWER(TRIM(value_written))
     FROM `proj-tmc-mem-com`.actionbuilder_sync.sync_log
     WHERE operation = 'insert_entity'
       AND status = 'ok'
       AND value_written IS NOT NULL
   ))
  -- Suppression guard: never insert a person on the curated do-not-sync list
  -- (actionbuilder_sync.suppression_list, managed via scripts/add_suppression.py)
  AND (email IS NULL
   OR LOWER(TRIM(email)) NOT IN (
     SELECT LOWER(TRIM(email))
     FROM `proj-tmc-mem-com`.actionbuilder_sync.suppression_list
     WHERE email IS NOT NULL
   ))
  AND (person_id IS NULL
   OR person_id NOT IN (
     SELECT person_id
     FROM `proj-tmc-mem-com`.actionbuilder_sync.suppression_list
     WHERE person_id IS NOT NULL
   ))
