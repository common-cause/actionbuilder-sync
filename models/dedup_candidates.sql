-- dedup_candidates: identifies ActionBuilder entities that are duplicates and should be deleted.
--
-- Output: one row per entity to delete, with a pointer to the canonical entity to keep.
-- Use delete_interact_id (36-char UUID) to delete entities via the ActionBuilder API.
-- If keep_interact_id IS NULL (test_account tier), delete with no replacement.
--
-- Five tiers:
--   person_id_match    — two entities resolve to the same core_enhanced person_id via email
--   name_email_match   — same first+last+email, no person_id (fallback for ~36 unmatched entities)
--   test_account       — gmail plus-alias accounts (e.g. foo+bar@gmail.com); delete outright
--   voterbase_id_match — same TargetSmart voter file ID + exact same name (auto-deletable)
--   resolved_merge     — MERGE decisions from dedup_resolutions table (human/AI reviewed)

-- ============================================================
-- Step 1: Primary email per entity
-- ============================================================
WITH primary_emails AS (
  SELECT
    owner_id AS entity_id,
    email,
    LOWER(TRIM(email)) AS email_norm,
    ROW_NUMBER() OVER (
      PARTITION BY owner_id
      ORDER BY
        CASE WHEN status = 'verified' THEN 1
             WHEN status = 'user_added' THEN 2
             ELSE 3 END,
        updated_at DESC
    ) AS email_rank
  FROM actionbuilder_cleaned.cln_actionbuilder__emails
  WHERE owner_type = 'Entity'
    AND status IN ('verified', 'user_added')
    AND email IS NOT NULL
),

-- ============================================================
-- Step 2: Tag count per entity (more tags = more sync history = prefer to keep)
-- ============================================================
tag_counts AS (
  SELECT
    taggable_id AS entity_id,
    COUNT(*) AS tag_count
  FROM actionbuilder_cleaned.cln_actionbuilder__taggable_logbook
  WHERE taggable_type = 'Entity'
    AND deleted_at IS NULL
    AND available = TRUE
  GROUP BY 1
),

-- ============================================================
-- Step 3: One person_id per entity via primary email
-- ROW_NUMBER ensures we pick exactly one person_id even when
-- core_enhanced has multiple person_ids for the same email
-- (can happen after record merges in the activist pool).
-- ============================================================
entity_person_ids AS (
  SELECT
    pe.entity_id,
    ep.person_id,
    ROW_NUMBER() OVER (PARTITION BY pe.entity_id ORDER BY ep.person_id) AS rn
  FROM primary_emails pe
  INNER JOIN core_enhanced.enh_activistpools__emails ep
    ON pe.email_norm = LOWER(TRIM(ep.email))
  WHERE pe.email_rank = 1
    AND ep.person_id IS NOT NULL
),

-- ============================================================
-- Step 3b: One voterbase_id per entity via person_id.
-- Provides a second, deeper identity signal: two entities that share
-- the same TargetSmart voter file record are very likely the same person.
-- Only exact-same-name pairs are auto-deleted here; name-divergent pairs
-- surface in dedup_ambiguous for human/AI review.
-- ============================================================
entity_voterbase_ids AS (
  SELECT
    epi.entity_id,
    eid.voterbase_id,
    ROW_NUMBER() OVER (PARTITION BY epi.entity_id ORDER BY eid.voterbase_id) AS rn
  FROM entity_person_ids epi
  INNER JOIN core_targetsmart_enhanced.enh_activistpools__identities eid
    ON epi.person_id = eid.person_id
  WHERE epi.rn = 1
    AND eid.voterbase_id IS NOT NULL
    AND eid.voterbase_id != ''
),

-- ============================================================
-- Step 3c: Active campaign memberships per entity (excluding Test).
-- Two uses:
--   (a) shared-campaign filter — only merge entities in the same campaign.
--       Cross-campaign pairs are left in place; they may represent the same
--       person operating across state programs.
--   (b) entity_any_campaign — one valid campaign interact_id for API calls.
-- ============================================================
entity_active_campaigns AS (
  SELECT
    ce.entity_id,
    c.interact_id AS campaign_interact_id
  FROM actionbuilder_cleaned.cln_actionbuilder__campaigns_entities ce
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns c
    ON ce.campaign_id = c.id
  WHERE c.status = 'active'
    AND c.name != 'Test'
),

entity_any_campaign AS (
  -- One deterministic campaign per entity (lexicographically first).
  -- As of Feb 2026, all entities are in exactly one active campaign.
  SELECT entity_id, campaign_interact_id
  FROM entity_active_campaigns
  QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY campaign_interact_id) = 1
),

-- ============================================================
-- Step 4: Combine entity base info with person_id, tag count,
-- and test-account flag
-- ============================================================
entities_enriched AS (
  SELECT
    e.id           AS entity_id,
    e.interact_id,
    e.first_name,
    e.last_name,
    e.created_at,
    pe.email,
    pe.email_norm,
    epi.person_id,
    COALESCE(tc.tag_count, 0) AS tag_count,

    -- Test accounts: gmail plus-aliases used for pipeline testing.
    -- These have no legitimate activist data and should be deleted outright.
    -- @commoncause.org staff emails are NOT flagged here; their duplicates
    -- are resolved by the person_id tier like any other entity.
    CASE
      WHEN REGEXP_CONTAINS(COALESCE(pe.email_norm, ''), r'^[^+]+\+[^@]+@gmail\.com$')
      THEN TRUE
      ELSE FALSE
    END AS is_test_account,

    evb.voterbase_id

  FROM actionbuilder_cleaned.cln_actionbuilder__entities e
  LEFT JOIN primary_emails pe
    ON pe.entity_id = e.id AND pe.email_rank = 1
  LEFT JOIN entity_person_ids epi
    ON epi.entity_id = e.id AND epi.rn = 1
  LEFT JOIN entity_voterbase_ids evb
    ON evb.entity_id = e.id AND evb.rn = 1
  LEFT JOIN tag_counts tc
    ON tc.entity_id = e.id
),

-- ============================================================
-- Step 5a: Rank within each person_id group (non-test entities only)
-- Keep rank = 1; delete ranks 2+
-- Tie-break: most tags first, then oldest
-- ============================================================
ranked_by_person_id AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY person_id
      ORDER BY tag_count DESC, created_at ASC
    ) AS rank_in_group,
    COUNT(*) OVER (PARTITION BY person_id) AS group_size
  FROM entities_enriched
  WHERE person_id IS NOT NULL
    AND is_test_account = FALSE
),

-- ============================================================
-- Step 5b: Rank within each name+email group (no person_id, non-test)
-- Fallback for the small number of entities that don't match core_enhanced
-- ============================================================
ranked_by_name_email AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        email_norm,
        LOWER(TRIM(COALESCE(first_name, ''))),
        LOWER(TRIM(COALESCE(last_name, '')))
      ORDER BY tag_count DESC, created_at ASC
    ) AS rank_in_group,
    COUNT(*) OVER (
      PARTITION BY
        email_norm,
        LOWER(TRIM(COALESCE(first_name, ''))),
        LOWER(TRIM(COALESCE(last_name, '')))
    ) AS group_size
  FROM entities_enriched
  WHERE person_id IS NULL
    AND email_norm IS NOT NULL
    AND first_name IS NOT NULL AND TRIM(first_name) != ''
    AND last_name  IS NOT NULL AND TRIM(last_name)  != ''
    AND is_test_account = FALSE
),

-- ============================================================
-- Step 6a: Person_id duplicate candidates (keep rank=1, delete rank>1)
-- ============================================================
person_id_candidates AS (
  SELECT
    del.interact_id                         AS delete_interact_id,
    keep.interact_id                        AS keep_interact_id,
    del.first_name                          AS delete_first_name,
    del.last_name                           AS delete_last_name,
    del.email                               AS delete_email,
    keep.email                              AS keep_email,
    del.person_id,
    'person_id_match'                       AS dedup_tier,
    CONCAT(
      'Shares person_id with ',
      keep.first_name, ' ', keep.last_name,
      ' — keeping entity with ', CAST(keep.tag_count AS STRING), ' tags',
      ', created ', CAST(DATE(keep.created_at) AS STRING)
    )                                       AS delete_reason,
    del.tag_count                           AS delete_tag_count,
    keep.tag_count                          AS keep_tag_count,
    DATE(del.created_at)                    AS delete_created_date,
    DATE(keep.created_at)                   AS keep_created_date,
    del.group_size
  FROM ranked_by_person_id del
  INNER JOIN ranked_by_person_id keep
    ON del.person_id = keep.person_id
    AND keep.rank_in_group = 1
  WHERE del.rank_in_group > 1
    -- Only merge entities that share at least one active campaign.
    -- Cross-state pairs (e.g. same person_id in AZ and CA) are excluded.
    AND EXISTS (
      SELECT 1
      FROM entity_active_campaigns eac_del
      INNER JOIN entity_active_campaigns eac_keep
        ON eac_del.campaign_interact_id = eac_keep.campaign_interact_id
      WHERE eac_del.entity_id = del.entity_id
        AND eac_keep.entity_id = keep.entity_id
    )
),

-- ============================================================
-- Step 6b: Name+email fallback candidates
-- ============================================================
name_email_candidates AS (
  SELECT
    del.interact_id                         AS delete_interact_id,
    keep.interact_id                        AS keep_interact_id,
    del.first_name                          AS delete_first_name,
    del.last_name                           AS delete_last_name,
    del.email                               AS delete_email,
    keep.email                              AS keep_email,
    CAST(NULL AS STRING)                    AS person_id,
    'name_email_match'                      AS dedup_tier,
    CONCAT(
      'Same name+email as ',
      keep.first_name, ' ', keep.last_name,
      ' (no person_id match) — keeping entity created ',
      CAST(DATE(keep.created_at) AS STRING)
    )                                       AS delete_reason,
    del.tag_count                           AS delete_tag_count,
    keep.tag_count                          AS keep_tag_count,
    DATE(del.created_at)                    AS delete_created_date,
    DATE(keep.created_at)                   AS keep_created_date,
    del.group_size
  FROM ranked_by_name_email del
  INNER JOIN ranked_by_name_email keep
    ON del.email_norm = keep.email_norm
    AND LOWER(TRIM(COALESCE(del.first_name, ''))) = LOWER(TRIM(COALESCE(keep.first_name, '')))
    AND LOWER(TRIM(COALESCE(del.last_name,  ''))) = LOWER(TRIM(COALESCE(keep.last_name,  '')))
    AND keep.rank_in_group = 1
  WHERE del.rank_in_group > 1
    AND EXISTS (
      SELECT 1
      FROM entity_active_campaigns eac_del
      INNER JOIN entity_active_campaigns eac_keep
        ON eac_del.campaign_interact_id = eac_keep.campaign_interact_id
      WHERE eac_del.entity_id = del.entity_id
        AND eac_keep.entity_id = keep.entity_id
    )
),

-- ============================================================
-- Step 6c: Test accounts — delete all instances (keep_interact_id = NULL)
-- Only catches gmail plus-alias emails that are NOT already handled
-- by the person_id tier above (they're excluded from ranked_by_person_id)
-- ============================================================
test_account_candidates AS (
  SELECT
    interact_id                             AS delete_interact_id,
    CAST(NULL AS STRING)                    AS keep_interact_id,
    first_name                              AS delete_first_name,
    last_name                               AS delete_last_name,
    email                                   AS delete_email,
    CAST(NULL AS STRING)                    AS keep_email,
    CAST(NULL AS STRING)                    AS person_id,
    'test_account'                          AS dedup_tier,
    CONCAT('Gmail plus-alias test account: ', COALESCE(email, '(no email)'))
                                            AS delete_reason,
    tag_count                               AS delete_tag_count,
    CAST(NULL AS INT64)                     AS keep_tag_count,
    DATE(created_at)                        AS delete_created_date,
    CAST(NULL AS DATE)                      AS keep_created_date,
    1                                       AS group_size
  FROM entities_enriched
  WHERE is_test_account = TRUE
),

-- ============================================================
-- Step 5c: Rank within each voterbase_id group (non-test entities only)
-- Only exact-same-name pairs auto-delete here; name-divergent pairs
-- surface in dedup_ambiguous instead.
-- ============================================================
ranked_by_voterbase_id AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY voterbase_id
      ORDER BY tag_count DESC, created_at ASC
    ) AS rank_in_group,
    COUNT(*) OVER (PARTITION BY voterbase_id) AS group_size
  FROM entities_enriched
  WHERE voterbase_id IS NOT NULL
    AND is_test_account = FALSE
),

-- ============================================================
-- Step 6d: Voterbase_id duplicate candidates — exact same name only
-- (Name-divergent voterbase pairs surface in dedup_ambiguous)
-- ============================================================
voterbase_id_candidates AS (
  SELECT
    del.interact_id                         AS delete_interact_id,
    keep.interact_id                        AS keep_interact_id,
    del.first_name                          AS delete_first_name,
    del.last_name                           AS delete_last_name,
    del.email                               AS delete_email,
    keep.email                              AS keep_email,
    del.person_id,
    'voterbase_id_match'                    AS dedup_tier,
    CONCAT(
      'Shares voterbase_id ', del.voterbase_id, ' with ',
      keep.first_name, ' ', keep.last_name,
      ' — keeping entity with ', CAST(keep.tag_count AS STRING), ' tags',
      ', created ', CAST(DATE(keep.created_at) AS STRING)
    )                                       AS delete_reason,
    del.tag_count                           AS delete_tag_count,
    keep.tag_count                          AS keep_tag_count,
    DATE(del.created_at)                    AS delete_created_date,
    DATE(keep.created_at)                   AS keep_created_date,
    del.group_size
  FROM ranked_by_voterbase_id del
  INNER JOIN ranked_by_voterbase_id keep
    ON del.voterbase_id = keep.voterbase_id
    AND keep.rank_in_group = 1
  WHERE del.rank_in_group > 1
    AND LOWER(TRIM(COALESCE(del.first_name, ''))) = LOWER(TRIM(COALESCE(keep.first_name, '')))
    AND LOWER(TRIM(COALESCE(del.last_name,  ''))) = LOWER(TRIM(COALESCE(keep.last_name,  '')))
    AND LOWER(TRIM(COALESCE(del.first_name, ''))) != ''
    AND LOWER(TRIM(COALESCE(del.last_name,  ''))) != ''
    AND EXISTS (
      SELECT 1
      FROM entity_active_campaigns eac_del
      INNER JOIN entity_active_campaigns eac_keep
        ON eac_del.campaign_interact_id = eac_keep.campaign_interact_id
      WHERE eac_del.entity_id = del.entity_id
        AND eac_keep.entity_id = keep.entity_id
    )
),

-- ============================================================
-- Step 6e: Resolved-merge candidates — MERGE decisions from the
-- dedup_resolutions BQ table (managed outside dbt, populated by
-- human or AI review of dedup_unresolved pairs).
-- Requires: scripts/create_dedup_resolutions.sql run first.
-- ============================================================
resolved_merge_candidates AS (
  SELECT
    dr.delete_interact_id,
    dr.keep_interact_id,
    del_e.first_name                        AS delete_first_name,
    del_e.last_name                         AS delete_last_name,
    del_pe.email                            AS delete_email,
    keep_pe.email                           AS keep_email,
    CAST(NULL AS STRING)                    AS person_id,
    'resolved_merge'                        AS dedup_tier,
    CONCAT(
      'Manual/AI resolution: ', COALESCE(dr.reason, '(no reason given)'),
      ' [resolved by ', COALESCE(dr.resolved_by, 'unknown'), ']'
    )                                       AS delete_reason,
    COALESCE(del_tc.tag_count, 0)           AS delete_tag_count,
    COALESCE(keep_tc.tag_count, 0)          AS keep_tag_count,
    DATE(del_e.created_at)                  AS delete_created_date,
    DATE(keep_e.created_at)                 AS keep_created_date,
    1                                       AS group_size
  FROM `proj-tmc-mem-com`.actionbuilder_sync.dedup_resolutions dr
  JOIN actionbuilder_cleaned.cln_actionbuilder__entities del_e
    ON dr.delete_interact_id = del_e.interact_id
  JOIN actionbuilder_cleaned.cln_actionbuilder__entities keep_e
    ON dr.keep_interact_id = keep_e.interact_id
  LEFT JOIN primary_emails del_pe
    ON del_pe.entity_id = del_e.id AND del_pe.email_rank = 1
  LEFT JOIN primary_emails keep_pe
    ON keep_pe.entity_id = keep_e.id AND keep_pe.email_rank = 1
  LEFT JOIN tag_counts del_tc ON del_tc.entity_id = del_e.id
  LEFT JOIN tag_counts keep_tc ON keep_tc.entity_id = keep_e.id
  WHERE dr.decision IN ('MERGE_A_INTO_B', 'MERGE_B_INTO_A')
    AND dr.delete_interact_id IS NOT NULL
    AND dr.keep_interact_id IS NOT NULL
    AND EXISTS (
      SELECT 1
      FROM entity_active_campaigns eac_del
      INNER JOIN entity_active_campaigns eac_keep
        ON eac_del.campaign_interact_id = eac_keep.campaign_interact_id
      WHERE eac_del.entity_id = del_e.id
        AND eac_keep.entity_id = keep_e.id
    )
)

-- ============================================================
-- Final output: union all tiers, deduplicate by delete_interact_id
-- (a test account entity could theoretically appear in multiple tiers
-- due to the person_id exclusion; QUALIFY guards against this)
-- ============================================================
SELECT
  delete_interact_id,
  keep_interact_id,
  delete_first_name,
  delete_last_name,
  delete_email,
  keep_email,
  person_id,
  dedup_tier,
  delete_reason,
  delete_tag_count,
  keep_tag_count,
  delete_created_date,
  keep_created_date,
  group_size,

  -- Campaign interact_id to use for the remove_records API call.
  -- Any active non-Test campaign the delete entity belongs to.
  -- NULL if the entity has no active non-Test campaign (cannot be API-deleted in this run).
  (
    SELECT MIN(eac.campaign_interact_id)
    FROM actionbuilder_cleaned.cln_actionbuilder__entities del_e
    INNER JOIN entity_active_campaigns eac ON eac.entity_id = del_e.id
    WHERE del_e.interact_id = delete_interact_id
  ) AS campaign_interact_id

FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY delete_interact_id ORDER BY
    CASE dedup_tier
      WHEN 'person_id_match'    THEN 1
      WHEN 'name_email_match'   THEN 2
      WHEN 'test_account'       THEN 3
      WHEN 'voterbase_id_match' THEN 4
      WHEN 'resolved_merge'     THEN 5
    END
  ) AS tier_rank
  FROM (
    SELECT * FROM person_id_candidates
    UNION ALL
    SELECT * FROM name_email_candidates
    UNION ALL
    SELECT * FROM test_account_candidates
    UNION ALL
    SELECT * FROM voterbase_id_candidates
    UNION ALL
    SELECT * FROM resolved_merge_candidates
  )
)
WHERE tier_rank = 1

ORDER BY
  CASE dedup_tier
    WHEN 'person_id_match'    THEN 1
    WHEN 'name_email_match'   THEN 2
    WHEN 'test_account'       THEN 3
    WHEN 'voterbase_id_match' THEN 4
    WHEN 'resolved_merge'     THEN 5
  END,
  delete_last_name,
  delete_first_name
