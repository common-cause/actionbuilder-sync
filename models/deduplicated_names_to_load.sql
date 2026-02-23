-- deduplicated_names_to_load: Final new-record insertion feed.
--
-- Takes master_load_qualifiers (people who qualify to be in AB but aren't yet)
-- and applies three layers of filtering / deduplication:
--
--   Layer 1 — AB exclusion: remove people already in ActionBuilder.
--     Checks by person_id (strongest — covers all their emails via the identity hub),
--     then by direct email match, then by phone for phone-only records.
--     Designed to run AFTER email_migration_needed and phone_migration_needed have been
--     applied, so keeper entities' contact sets already include migrated secondary info.
--     Also holds out records whose voterbase_id is involved in an unresolved dedup pair
--     (dedup_unresolved). Prevents creating a third entity while two existing ones are
--     pending human/AI review.
--
--   Layer 2 — Test account exclusion: strip gmail plus-alias emails from the incoming
--     feed (same logic as dedup_candidates). These should never be created in AB.
--
--   Layer 3 — Within-feed dedup (two passes):
--
--     Pass A — Canonical email dedup: catches duplicates the person_id grouping in
--       master_load_qualifiers cannot resolve.
--         - Gmail addresses: normalize by removing dots and plus-aliases from the local
--           part (j.smith+test@gmail.com and jsmith@gmail.com are the same mailbox).
--         - Non-gmail: strip plus-aliases only.
--       For each canonical email group, keep the row with the most qualification sources.
--
--     Pass B — Name + phone dedup: catches same person appearing with different emails
--       (or no email) but identical normalized first name + last name + 10-digit phone.
--       Only applied when all three fields are present; records missing any field pass
--       through unchanged. This goes beyond person_id matching — the identity hub uses
--       a conservative match process that can leave the same real person in two separate
--       chains. Name+phone is a high-confidence signal to merge those.
--
-- Order of operations:
--   1. Run email_migration_needed via sync script (prepare_email_data)
--   2. Run phone_migration_needed via sync script (prepare_phone_data)
--   3. Run dedup_candidates deletions via sync script (remove_records)
--   4. Resolve open dedup_unresolved pairs to unblock held-out records
--   5. Use this view as the new-record insertion feed (insert_new_records)

-- ============================================================
-- Layer 1: Build AB exclusion sets
-- These reflect the full consolidated contact sets, including any secondary
-- emails/phones migrated from deleted entities to keeper entities.
-- ============================================================
WITH ab_emails AS (
  -- Every email address currently on any AB entity
  SELECT LOWER(TRIM(email)) AS email_norm
  FROM actionbuilder_cleaned.cln_actionbuilder__emails
  WHERE owner_type = 'Entity'
    AND email IS NOT NULL
    AND status IN ('verified', 'user_added')
),

ab_phones AS (
  -- Every phone number currently on any AB entity, normalized to 10 digits.
  -- Used only to exclude phone-only qualifier records (no email) already in AB.
  SELECT
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(number, r'^\+', ''), r'^1', ''), r'[^\d]', '') AS phone_norm
  FROM actionbuilder_cleaned.cln_actionbuilder__phone_numbers
  WHERE owner_type = 'Entity'
    AND number IS NOT NULL
    AND status IN ('verified', 'user_added')
    AND LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(number, r'^\+', ''), r'^1', ''), r'[^\d]', '')) = 10
),

ab_person_ids AS (
  -- Every person_id that already has at least one AB entity, resolved via any
  -- email currently on that entity (including migrated secondary emails).
  SELECT DISTINCT ep.person_id
  FROM actionbuilder_cleaned.cln_actionbuilder__emails abe
  INNER JOIN core_enhanced.enh_activistpools__emails ep
    ON LOWER(TRIM(abe.email)) = LOWER(TRIM(ep.email))
  WHERE abe.owner_type = 'Entity'
    AND abe.email IS NOT NULL
    AND ep.person_id IS NOT NULL
),

-- ============================================================
-- Hold-out: voterbase_ids involved in unresolved dedup pairs.
-- New records that map to these voterbase_ids are withheld until
-- the ambiguity between existing AB entities is resolved.
-- ============================================================
unresolved_voterbase_ids AS (
  SELECT entity_a_voterbase_id AS voterbase_id
  FROM {{ ref('dedup_unresolved') }}
  WHERE entity_a_voterbase_id IS NOT NULL
  UNION DISTINCT
  SELECT entity_b_voterbase_id AS voterbase_id
  FROM {{ ref('dedup_unresolved') }}
  WHERE entity_b_voterbase_id IS NOT NULL
),

incoming_voterbase_ids AS (
  SELECT
    mlq.person_id,
    eid.voterbase_id
  FROM {{ ref('master_load_qualifiers') }} mlq
  INNER JOIN core_targetsmart_enhanced.enh_activistpools__identities eid
    ON mlq.person_id = eid.person_id
  WHERE mlq.person_id IS NOT NULL
    AND eid.voterbase_id IS NOT NULL
    AND eid.voterbase_id != ''
),

held_out_person_ids AS (
  SELECT DISTINCT ivb.person_id
  FROM incoming_voterbase_ids ivb
  INNER JOIN unresolved_voterbase_ids uvb
    ON ivb.voterbase_id = uvb.voterbase_id
),

-- ============================================================
-- Layers 1 + 2 applied: base qualified set after AB exclusion and test account removal
-- ============================================================
base_qualified AS (
  SELECT
    mlq.person_id,
    mlq.first_name,
    mlq.last_name,
    mlq.phone_number,
    mlq.email,
    COALESCE(s.abbreviation, mlq.state)   AS state,
    mlq.county,
    mlq.zip_code,
    mlq.source_code,
    mlq.created_at,
    mlq.shifted_2024,
    mlq.events_6m,
    mlq.phone_bank_dials,
    mlq.action_network_actions,
    mlq.action_network_field,
    mlq.events_field,
    mlq.pb_field,
    mlq.first_event_field,
    mlq.mr_event_field,
    mlq.first_event_date,
    mlq.mr_event_date,
    mlq.qualification_count

  FROM {{ ref('master_load_qualifiers') }} mlq
  LEFT JOIN actionnetwork_views.states s
    ON mlq.state = s.name OR mlq.state = s.abbreviation

  WHERE (mlq.email IS NOT NULL OR mlq.phone_number IS NOT NULL)
    AND s.abbreviation IS NOT NULL

    -- Exclude by person_id: covers all emails the identity hub has ever seen
    -- for this person, including across multiple source platforms.
    AND (
      mlq.person_id IS NULL
      OR mlq.person_id NOT IN (SELECT person_id FROM ab_person_ids)
    )

    -- Exclude by direct email: catches unmatched records (no person_id)
    -- whose email is literally already in AB.
    AND (
      mlq.email IS NULL
      OR LOWER(TRIM(mlq.email)) NOT IN (SELECT email_norm FROM ab_emails)
    )

    -- Exclude phone-only records (no email) whose phone is already in AB.
    -- We don't use phone to exclude email-bearing records — phones are shared
    -- more often than emails (family members, recycled numbers).
    AND NOT (
      mlq.email IS NULL
      AND mlq.phone_number IS NOT NULL
      AND REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(mlq.phone_number, r'^\+', ''), r'^1', ''), r'[^\d]', '')
          IN (SELECT phone_norm FROM ab_phones)
    )

    -- Exclude test accounts: gmail plus-aliases should never be created in AB.
    AND NOT REGEXP_CONTAINS(
      LOWER(TRIM(COALESCE(mlq.email, ''))),
      r'^[^+]+\+[^@]+@gmail\.com$'
    )

    -- Hold out: do not create a new entity for a person whose existing
    -- AB entities are in an unresolved dedup pair. Prevents a third entity
    -- from being created before the ambiguity is resolved.
    AND (
      mlq.person_id IS NULL
      OR mlq.person_id NOT IN (SELECT person_id FROM held_out_person_ids)
    )
),

-- ============================================================
-- Layer 3A: Compute canonical keys for within-feed dedup
-- ============================================================
with_dedup_keys AS (
  SELECT
    *,

    -- Canonical email: Gmail-normalized (dots removed, plus-alias stripped, googlemail→gmail).
    -- Non-gmail: plus-alias stripped only.
    -- NULL for phone-only records.
    CASE
      WHEN REGEXP_CONTAINS(LOWER(TRIM(COALESCE(email, ''))), r'@(gmail|googlemail)\.com$')
      THEN CONCAT(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            LOWER(TRIM(REGEXP_EXTRACT(COALESCE(email, ''), r'^([^@]+)'))),
            r'\+.*$', ''          -- strip +alias
          ),
          r'\.', ''               -- remove all dots
        ),
        '@gmail.com'              -- normalize googlemail -> gmail
      )
      WHEN email IS NOT NULL AND TRIM(email) != ''
      THEN REGEXP_REPLACE(LOWER(TRIM(email)), r'\+[^@]*@', '@')
      ELSE NULL
    END AS email_canonical,

    -- Name + phone composite key: only set when all three fields are present and valid.
    -- Used as Pass B dedup key to catch same person with different emails.
    CASE
      WHEN first_name IS NOT NULL AND TRIM(first_name) != ''
        AND last_name  IS NOT NULL AND TRIM(last_name)  != ''
        AND phone_number IS NOT NULL
        AND LENGTH(
          REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(phone_number, r'^\+', ''), r'^1', ''), r'[^\d]', '')
        ) = 10
      THEN CONCAT(
        LOWER(TRIM(first_name)), '||',
        LOWER(TRIM(last_name)),  '||',
        REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(phone_number, r'^\+', ''), r'^1', ''), r'[^\d]', '')
      )
      ELSE NULL
    END AS name_phone_key

  FROM base_qualified
),

-- ============================================================
-- Layer 3 Pass A: Dedup by canonical email
--
-- For each canonical email, keep the row with the most qualification sources.
-- Tiebreakers: EP shift, name completeness, oldest record.
-- Phone-only records (NULL email_canonical) each get a unique sentinel so
-- they are never collapsed together.
-- ============================================================
email_deduped AS (
  SELECT *
  FROM with_dedup_keys
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY COALESCE(
      email_canonical,
      CONCAT('phone_only::', COALESCE(
        REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(phone_number, r'^\+', ''), r'^1', ''), r'[^\d]', ''),
        COALESCE(person_id, 'unknown')
      ))
    )
    ORDER BY
      qualification_count DESC,
      CASE WHEN shifted_2024 = 'Y' THEN 1 ELSE 2 END,
      CASE WHEN first_name IS NOT NULL AND last_name IS NOT NULL THEN 1 ELSE 2 END,
      created_at ASC
  ) = 1
),

-- ============================================================
-- Layer 3 Pass B: Dedup by name + phone
--
-- For each (first_name, last_name, normalized_phone) group, keep the row
-- with the most qualification sources. This resolves cases where the
-- identity hub's conservative matching left the same real person as two
-- separate person_id chains with different emails.
--
-- Only applied to records where all three fields are present (name_phone_key IS NOT NULL).
-- Records missing any field pass through the UNION ALL branch unchanged —
-- we don't want to accidentally merge unrelated phone-only or nameless records.
-- ============================================================
name_phone_keyed AS (
  SELECT *
  FROM email_deduped
  WHERE name_phone_key IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY name_phone_key
    ORDER BY
      qualification_count DESC,
      CASE WHEN shifted_2024 = 'Y' THEN 1 ELSE 2 END,
      CASE WHEN email IS NOT NULL THEN 1 ELSE 2 END,   -- prefer record with email when tying
      created_at ASC
  ) = 1
),

name_phone_deduped AS (
  -- Records with a complete name+phone key, deduplicated
  SELECT * FROM name_phone_keyed

  UNION ALL

  -- Records missing name or phone: pass through unchanged
  SELECT * FROM email_deduped
  WHERE name_phone_key IS NULL
)

-- ============================================================
-- Final output
-- GROUP BY collapses any residual rows with identical contact info
-- (e.g. same person, different source_code — pick min created_at, max AN actions)
-- ============================================================
SELECT
  first_name,
  last_name,
  phone_number,
  email,
  state,
  county,
  zip_code,
  source_code,
  MIN(created_at)                AS created_at,
  shifted_2024,
  events_6m,
  phone_bank_dials,
  MAX(action_network_actions)    AS action_network_actions,

  action_network_field,
  events_field,
  pb_field,
  first_event_field,
  mr_event_field,
  first_event_date,
  mr_event_date

FROM name_phone_deduped

GROUP BY
  first_name, last_name, phone_number, email, state, county, zip_code, source_code,
  shifted_2024, events_6m, phone_bank_dials,
  action_network_field, events_field, pb_field, first_event_field, mr_event_field,
  first_event_date, mr_event_date

ORDER BY
  CASE WHEN shifted_2024 = 'Y' THEN 1 ELSE 2 END,
  last_name, first_name
