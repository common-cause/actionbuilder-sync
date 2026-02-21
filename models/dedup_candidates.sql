-- dedup_candidates: identifies ActionBuilder entities that are duplicates and should be deleted.
--
-- Output: one row per entity to delete, with a pointer to the canonical entity to keep.
-- Use delete_interact_id (36-char UUID) to delete entities via the ActionBuilder API.
-- If keep_interact_id IS NULL (test_account tier), delete with no replacement.
--
-- Three tiers:
--   person_id_match  — two entities resolve to the same core_enhanced person_id via email
--   name_email_match — same first+last+email, no person_id (fallback for ~36 unmatched entities)
--   test_account     — gmail plus-alias accounts (e.g. foo+bar@gmail.com); delete outright

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
    END AS is_test_account

  FROM actionbuilder_cleaned.cln_actionbuilder__entities e
  LEFT JOIN primary_emails pe
    ON pe.entity_id = e.id AND pe.email_rank = 1
  LEFT JOIN entity_person_ids epi
    ON epi.entity_id = e.id AND epi.rn = 1
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
  group_size

FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY delete_interact_id ORDER BY
    CASE dedup_tier
      WHEN 'person_id_match'  THEN 1
      WHEN 'name_email_match' THEN 2
      WHEN 'test_account'     THEN 3
    END
  ) AS tier_rank
  FROM (
    SELECT * FROM person_id_candidates
    UNION ALL
    SELECT * FROM name_email_candidates
    UNION ALL
    SELECT * FROM test_account_candidates
  )
)
WHERE tier_rank = 1

ORDER BY
  CASE dedup_tier
    WHEN 'person_id_match'  THEN 1
    WHEN 'name_email_match' THEN 2
    WHEN 'test_account'     THEN 3
  END,
  delete_last_name,
  delete_first_name
