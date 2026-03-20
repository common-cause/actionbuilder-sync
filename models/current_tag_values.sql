-- current_tag_values: overlay wrapper
--
-- Merges the BQ snapshot (from taggable_logbook via current_tag_values_bq_only)
-- with our sync_log entries to reconstruct true tag state when taggable_logbook
-- replication is stale.
--
-- Logic:
--   For each entity + campaign + tag_name:
--     1. If sync_log has activity → most recent log entry wins:
--        - add_tagging → tag is present with value_written
--        - delete_tagging with no subsequent add → tag cleared (excluded)
--     2. If no sync_log activity → BQ snapshot row survives as-is
--
-- To revert to BQ-only mode when AB fixes taggable_logbook:
--   1. Delete this file
--   2. Rename current_tag_values_bq_only.sql → current_tag_values.sql
--   3. bash dbt.sh run

WITH bq_snapshot AS (
  -- The BQ-only view gives us the taggable_logbook-based state
  SELECT
    entity_id,
    campaign_id,
    tag_id,
    tag_interact_id,
    tag_name,
    tag_type,
    tag_category_id,
    taggable_logbook_interact_id,
    current_value,
    tag_applied_at,
    removal_string,
    sync_field_identifier
  FROM {{ ref('current_tag_values_bq_only') }}
),

-- Bridge tables: map interact_id UUIDs in sync_log to internal int IDs used by BQ snapshot
entity_ids AS (
  SELECT id AS entity_id_int, interact_id AS entity_interact_id
  FROM actionbuilder_cleaned.cln_actionbuilder__entities
),
campaign_ids AS (
  SELECT id AS campaign_id_int, interact_id AS campaign_interact_id
  FROM actionbuilder_cleaned.cln_actionbuilder__campaigns
),

-- All tag-level sync_log entries (add_tagging and delete_tagging)
-- with status ok or 404 (both represent achieved state)
sync_log_tags AS (
  SELECT
    sl.entity_interact_id,
    sl.campaign_interact_id,
    sl.tag_name,
    sl.tag_interact_id,
    sl.value_written,
    sl.tagging_interact_id,
    sl.operation,
    sl.executed_at,
    ROW_NUMBER() OVER (
      PARTITION BY sl.entity_interact_id, sl.campaign_interact_id, sl.tag_name
      ORDER BY sl.executed_at DESC
    ) AS rn
  FROM `proj-tmc-mem-com`.actionbuilder_sync.sync_log sl
  WHERE sl.operation IN ('add_tagging', 'delete_tagging')
    AND sl.status IN ('ok', '404')
    AND sl.tag_name IS NOT NULL
),

-- Most recent sync_log entry per entity+campaign+tag
latest_sync_log AS (
  SELECT
    entity_interact_id,
    campaign_interact_id,
    tag_name,
    tag_interact_id,
    value_written,
    tagging_interact_id,
    operation,
    executed_at
  FROM sync_log_tags
  WHERE rn = 1
),

-- Tags in BQ snapshot that have sync_log activity
-- These get REPLACED by sync_log state
bq_with_overlay AS (
  SELECT
    bs.entity_id,
    bs.campaign_id,
    bs.tag_id,
    bs.tag_interact_id,
    bs.tag_name,
    bs.tag_type,
    bs.tag_category_id,
    bs.taggable_logbook_interact_id,
    bs.current_value,
    bs.tag_applied_at,
    bs.removal_string,
    bs.sync_field_identifier,
    lsl.operation AS log_operation,
    lsl.value_written AS log_value,
    lsl.tagging_interact_id AS log_tagging_iid,
    lsl.executed_at AS log_executed_at
  FROM bq_snapshot bs
  LEFT JOIN entity_ids ei ON ei.entity_id_int = bs.entity_id
  LEFT JOIN campaign_ids ci ON ci.campaign_id_int = bs.campaign_id
  LEFT JOIN latest_sync_log lsl
    ON lsl.entity_interact_id = ei.entity_interact_id
    AND lsl.tag_name = bs.tag_name
    AND lsl.campaign_interact_id = ci.campaign_interact_id
),

-- Tags that only exist in sync_log (no BQ snapshot row yet — e.g. newly written tags
-- that taggable_logbook hasn't picked up)
sync_only_tags AS (
  SELECT
    lsl.entity_interact_id,
    lsl.campaign_interact_id,
    lsl.tag_name,
    lsl.tag_interact_id,
    lsl.value_written,
    lsl.tagging_interact_id,
    lsl.operation,
    lsl.executed_at,
    ei.entity_id_int AS entity_id,
    ci.campaign_id_int AS campaign_id
  FROM latest_sync_log lsl
  JOIN entity_ids ei ON ei.entity_interact_id = lsl.entity_interact_id
  JOIN campaign_ids ci ON ci.campaign_interact_id = lsl.campaign_interact_id
  -- Exclude tags that already exist in BQ snapshot (handled by bq_with_overlay)
  LEFT JOIN bq_snapshot bs
    ON bs.entity_id = ei.entity_id_int
    AND bs.campaign_id = ci.campaign_id_int
    AND bs.tag_name = lsl.tag_name
  WHERE bs.entity_id IS NULL
    AND lsl.operation = 'add_tagging'  -- Only adds create new state; deletes of non-existent are no-ops
),

-- Tag metadata lookup for sync-only tags
tag_meta AS (
  SELECT id, interact_id, name, tag_type, tag_category_id
  FROM actionbuilder_cleaned.cln_actionbuilder__tags
  WHERE status = 1
),

-- UNION 1: BQ snapshot rows where sync_log says add_tagging (override value)
overlay_adds AS (
  SELECT
    entity_id,
    campaign_id,
    tag_id,
    tag_interact_id,
    tag_name,
    tag_type,
    tag_category_id,
    -- Use sync_log tagging_interact_id if available, else fall back to BQ
    COALESCE(log_tagging_iid, taggable_logbook_interact_id) AS taggable_logbook_interact_id,
    log_value AS current_value,
    log_executed_at AS tag_applied_at,
    -- Rebuild removal_string with sync_log data if tagging_iid available
    CASE
      WHEN log_tagging_iid IS NOT NULL
      THEN CONCAT(tag_interact_id, ':|:', log_tagging_iid)
      ELSE removal_string
    END AS removal_string,
    sync_field_identifier
  FROM bq_with_overlay
  WHERE log_operation = 'add_tagging'
),

-- UNION 2: BQ snapshot rows with no sync_log activity (pass through)
passthrough AS (
  SELECT
    entity_id,
    campaign_id,
    tag_id,
    tag_interact_id,
    tag_name,
    tag_type,
    tag_category_id,
    taggable_logbook_interact_id,
    current_value,
    tag_applied_at,
    removal_string,
    sync_field_identifier
  FROM bq_with_overlay
  WHERE log_operation IS NULL
),

-- UNION 3: Sync-only tags (not in BQ snapshot)
new_from_sync AS (
  SELECT
    sot.entity_id,
    sot.campaign_id,
    tm.id AS tag_id,
    COALESCE(sot.tag_interact_id, tm.interact_id) AS tag_interact_id,
    sot.tag_name,
    tm.tag_type,
    tm.tag_category_id,
    sot.tagging_interact_id AS taggable_logbook_interact_id,
    sot.value_written AS current_value,
    sot.executed_at AS tag_applied_at,
    -- Build removal_string if we have both IDs
    CASE
      WHEN COALESCE(sot.tag_interact_id, tm.interact_id) IS NOT NULL
        AND sot.tagging_interact_id IS NOT NULL
      THEN CONCAT(COALESCE(sot.tag_interact_id, tm.interact_id), ':|:', sot.tagging_interact_id)
      ELSE NULL
    END AS removal_string,
    -- Build sync_field_identifier from tag_name
    CASE
      WHEN sot.tag_name = 'Events Attended Past 6 Months'          THEN 'Participation:|:Event Attendance Summary:|:Events Attended Past 6 Months:|:number_response'
      WHEN sot.tag_name = 'Most Recent Event Attended'              THEN 'Participation:|:Event Attendance History:|:Most Recent Event Attended:|:date_response'
      WHEN sot.tag_name = 'First Event Attended'                    THEN 'Participation:|:Event Attendance History:|:First Event Attended:|:date_response'
      WHEN sot.tag_name = 'Action Network Actions'                  THEN 'Participation:|:Online Actions Past 6 Months:|:Action Network Actions:|:number_response'
      WHEN sot.tag_name = 'Action Network State Actions'            THEN 'Participation:|:Online Actions Past 6 Months:|:Action Network State Actions:|:number_response'
      WHEN sot.tag_name = 'Top State Action Taker'                  THEN 'Participation:|:State Online Actions:|:Top State Action Taker:|:standard_response'
      WHEN sot.tag_name = 'Phone Bank Calls Made'                   THEN 'Participation:|:Event Attendance Summary:|:Phone Bank Calls Made:|:number_response'
      WHEN sot.tag_name = 'NewMode Actions'                         THEN 'Participation:|:Online Actions Past 6 Months:|:NewMode Actions:|:number_response'
      WHEN sot.tag_name = 'Top National Action Network Activist'    THEN 'Participation:|:National Online Actions:|:Top National Action Network Activist:|:standard_response'
      WHEN sot.tag_name = 'Hot Prospect'                            THEN 'Engagement:|:Prospect Identification:|:Hot Prospect:|:standard_response'
      ELSE CONCAT(sot.tag_name, ':|:standard_response')
    END AS sync_field_identifier
  FROM sync_only_tags sot
  LEFT JOIN tag_meta tm ON tm.name = sot.tag_name
)

-- BQ snapshot rows with no sync_log override (most rows, when log is sparse)
-- are excluded via WHERE log_operation = 'delete_tagging' (they get dropped)

SELECT * FROM overlay_adds
UNION ALL
SELECT * FROM passthrough
UNION ALL
SELECT * FROM new_from_sync
ORDER BY entity_id, campaign_id, tag_name
