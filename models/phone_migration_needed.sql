-- phone_migration_needed: Secondary phone numbers to add to keeper entities before executing dedup deletions.
--
-- One row per phone number to migrate.
-- To be consumed by sync script using the prepare_phone_data operation.
--
-- Mirrors email_migration_needed exactly — same logic, same tiers, same anti-join pattern.
--
-- Run this (alongside email_migration_needed) BEFORE executing remove_records deletions.
-- After migration, the keeper entity's consolidated phone set ensures correct_participation_values
-- continues to aggregate ScaleToWin call data across all phone numbers.

-- ============================================================
-- Step 0: One active campaign per keeper entity (for API calls).
-- Mirrors email_migration_needed pattern exactly.
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
-- Step 2: All phones on entities being deleted (valid 10-digit only)
-- We keep the original number string for the API call and the normalized
-- form for the anti-join comparison.
-- ============================================================
all_delete_phones AS (
  SELECT
    dem.delete_interact_id,
    abp.number      AS phone_number,
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(abp.number, r'^\+', ''), r'^1', ''), r'[^\d]', '') AS phone_norm
  FROM delete_entity_map dem
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__phone_numbers abp
    ON dem.entity_id = abp.owner_id
  WHERE abp.owner_type = 'Entity'
    AND abp.number IS NOT NULL
    AND abp.status IN ('verified', 'user_added')
    AND LENGTH(
      REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(abp.number, r'^\+', ''), r'^1', ''), r'[^\d]', '')
    ) = 10
),

-- ============================================================
-- Step 3: All phones already on keeper entities
-- Used for the anti-join below to avoid duplicating phone numbers
-- ============================================================
all_keeper_phones AS (
  SELECT
    kem.keep_interact_id,
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(abp.number, r'^\+', ''), r'^1', ''), r'[^\d]', '') AS phone_norm
  FROM keeper_entity_map kem
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__phone_numbers abp
    ON kem.entity_id = abp.owner_id
  WHERE abp.owner_type = 'Entity'
    AND abp.number IS NOT NULL
    AND abp.status IN ('verified', 'user_added')
    AND LENGTH(
      REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(abp.number, r'^\+', ''), r'^1', ''), r'[^\d]', '')
    ) = 10
)

-- ============================================================
-- Final: phones to migrate, excluding what the keeper already has
-- ============================================================
SELECT
  dc.keep_interact_id   AS entity_id,       -- entity that will receive the new secondary phone
  dc.delete_interact_id,                     -- audit trail: which entity this phone comes from
  dc.dedup_tier,
  kc.campaign_interact_id,                   -- campaign for the update_person API call
  adp.phone_number      AS phone_to_add,     -- original string as stored in AB (pass to API as-is)
  adp.phone_norm                             -- normalized 10-digit form (for debugging / dedup checks)

FROM {{ ref('dedup_candidates') }} dc
INNER JOIN all_delete_phones adp
  ON dc.delete_interact_id = adp.delete_interact_id

-- Anti-join: only include phones NOT already on the keeper entity
LEFT JOIN all_keeper_phones akp
  ON  dc.keep_interact_id = akp.keep_interact_id
  AND adp.phone_norm      = akp.phone_norm

LEFT JOIN keeper_campaign kc
  ON dc.keep_interact_id = kc.keep_interact_id

WHERE dc.keep_interact_id IS NOT NULL        -- skip test_account tier
  AND akp.keep_interact_id IS NULL           -- not already present on keeper

ORDER BY
  dc.keep_interact_id,
  adp.phone_norm
