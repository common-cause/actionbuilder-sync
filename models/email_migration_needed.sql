-- email_migration_needed: Secondary emails to add to keeper entities before executing dedup deletions.
--
-- One row per email to migrate.
-- To be consumed by sync script using the prepare_email_data operation.
--
-- Logic:
--   For each (delete, keep) pair in dedup_candidates (person_id_match and name_email_match tiers only):
--     - Collect ALL emails from the delete entity (not just the primary)
--     - Exclude any email already present on the keeper entity
--     - Output remaining emails as new secondary emails to add to the keeper
--
-- Run this BEFORE executing dedup deletions. Confirm emails have been added,
-- then run the remove_records / delete step.

-- ============================================================
-- Step 0: One active campaign per keeper entity (for API calls).
-- The sync script needs a valid campaign_id for every update_person call.
-- As of Feb 2026, every entity belongs to exactly one active non-Test campaign.
-- ============================================================
WITH keeper_campaign AS (
  SELECT
    e.interact_id       AS keep_interact_id,
    MIN(c.interact_id)  AS campaign_interact_id
  FROM actionbuilder_cleaned.cln_actionbuilder__entities e
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns_entities ce
    ON e.id = ce.entity_id
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns c
    ON ce.campaign_id = c.id
  WHERE c.status = 'active'
    AND c.name != 'Test'
  GROUP BY e.interact_id
),

-- ============================================================
-- Step 1: Map interact_ids to internal entity IDs
-- ============================================================
delete_entity_map AS (
  SELECT
    e.id            AS entity_id,
    e.interact_id   AS delete_interact_id
  FROM actionbuilder_cleaned.cln_actionbuilder__entities e
  INNER JOIN {{ ref('dedup_candidates') }} dc
    ON e.interact_id = dc.delete_interact_id
  WHERE dc.keep_interact_id IS NOT NULL   -- test_account tier has no keeper; skip
),

keeper_entity_map AS (
  SELECT
    e.id            AS entity_id,
    e.interact_id   AS keep_interact_id
  FROM actionbuilder_cleaned.cln_actionbuilder__entities e
  INNER JOIN {{ ref('dedup_candidates') }} dc
    ON e.interact_id = dc.keep_interact_id
),

-- ============================================================
-- Step 2: All emails on entities being deleted
-- ============================================================
all_delete_emails AS (
  SELECT
    dem.delete_interact_id,
    abe.email,
    LOWER(TRIM(abe.email))  AS email_norm
  FROM delete_entity_map dem
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__emails abe
    ON dem.entity_id = abe.owner_id
  WHERE abe.owner_type = 'Entity'
    AND abe.email     IS NOT NULL
    AND abe.status    IN ('verified', 'user_added')
),

-- ============================================================
-- Step 3: All emails already on keeper entities
-- Used for the anti-join below to avoid duplicating emails
-- ============================================================
all_keeper_emails AS (
  SELECT
    kem.keep_interact_id,
    LOWER(TRIM(abe.email))  AS email_norm
  FROM keeper_entity_map kem
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__emails abe
    ON kem.entity_id = abe.owner_id
  WHERE abe.owner_type = 'Entity'
    AND abe.email     IS NOT NULL
    AND abe.status    IN ('verified', 'user_added')
)

-- ============================================================
-- Final: emails to migrate, excluding what the keeper already has
-- ============================================================
SELECT
  dc.keep_interact_id   AS entity_id,       -- entity that will receive the new secondary email
  dc.delete_interact_id,                     -- audit trail: which entity this email comes from
  dc.dedup_tier,
  kc.campaign_interact_id,                   -- campaign for the update_person API call
  ade.email             AS email_to_add,
  ade.email_norm

FROM {{ ref('dedup_candidates') }} dc
INNER JOIN all_delete_emails ade
  ON dc.delete_interact_id = ade.delete_interact_id

-- Anti-join: only include emails NOT already on the keeper entity
LEFT JOIN all_keeper_emails ake
  ON  dc.keep_interact_id = ake.keep_interact_id
  AND ade.email_norm      = ake.email_norm

LEFT JOIN keeper_campaign kc
  ON dc.keep_interact_id = kc.keep_interact_id

WHERE dc.keep_interact_id IS NOT NULL        -- skip test_account tier
  AND ake.keep_interact_id IS NULL           -- not already present on keeper

ORDER BY
  dc.keep_interact_id,
  ade.email
