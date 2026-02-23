-- Materialized as a table (not a view) so that downstream views
-- (dedup_unresolved → deduplicated_names_to_load) do not inherit its
-- complex SQL chain, which would exceed BigQuery's query planning limits.
{{ config(materialized='table') }}

-- dedup_ambiguous: entity pairs that are possible duplicates but cannot be
-- definitively resolved by the automated logic in dedup_candidates.
--
-- Two signals:
--
--   voterbase_id_diff_name
--     Two entities share the same TargetSmart voter file ID (via
--     entity → primary email → person_id → voterbase_id) but do NOT have
--     the exact same normalized full name (first + last). This includes:
--       - Same last name, different first name (Robert Druessel / Rob Druessel,
--         Richard Dresser / Sylvia Dresser — spouses show up here too)
--       - Different last name entirely (Kelly Dufour / Mia Lewis)
--       - Corrupted name fields (A B / Alexis Barksdale, Laurine Laurine Cooke)
--     Pairs with EXACTLY the same normalized first + last name go directly to
--     dedup_candidates (voterbase_id_match tier) — they are auto-deletable.
--
--   shared_phone_same_lastname
--     Two entities share the same 10-digit phone number and the same normalized
--     last name, and are not already captured by another dedup signal.
--     Could be: same person registered twice with different emails, OR family
--     members sharing a household phone with the same surname.
--     NOTE: review these conservatively — flag as KEEP_BOTH for household phones.
--     After ~50 resolutions, evaluate whether this signal is net-useful.
--
-- Each pair appears exactly once, canonicalized so that
-- LEAST(interact_id) = entity_a, GREATEST(interact_id) = entity_b.
--
-- Resolution flow:
--   1. Review pairs in dedup_unresolved (this view minus already-resolved pairs).
--   2. Write a decision to actionbuilder_sync.dedup_resolutions using
--      scripts/add_resolution.py (or direct INSERT).
--   3. On next dbt run:
--      - MERGE decisions flow into dedup_candidates (resolved_merge tier)
--      - Pair disappears from dedup_unresolved
--      - Held-out new records in deduplicated_names_to_load are unblocked

-- ============================================================
-- Entity base info — primary email, primary phone, tags, person_id, voterbase_id
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

primary_phones AS (
  SELECT
    owner_id AS entity_id,
    number    AS phone_raw,
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(number, r'^[+]1', ''), r'^[+]', ''), r'[^0-9]', '') AS phone_norm,
    ROW_NUMBER() OVER (
      PARTITION BY owner_id
      ORDER BY
        CASE WHEN status = 'verified' THEN 1
             WHEN status = 'user_added' THEN 2
             ELSE 3 END,
        updated_at DESC
    ) AS phone_rank
  FROM actionbuilder_cleaned.cln_actionbuilder__phone_numbers
  WHERE owner_type = 'Entity'
    AND status IN ('verified', 'user_added')
    AND number IS NOT NULL
),

-- All phones per entity: used to detect the shared-phone signal.
-- We check every phone on each entity, not just the primary.
all_ab_phones AS (
  SELECT
    owner_id AS entity_id,
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(number, r'^[+]1', ''), r'^[+]', ''), r'[^0-9]', '') AS phone_norm
  FROM actionbuilder_cleaned.cln_actionbuilder__phone_numbers
  WHERE owner_type = 'Entity'
    AND status IN ('verified', 'user_added')
    AND number IS NOT NULL
),

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

primary_addresses AS (
  SELECT
    owner_id AS entity_id,
    state,
    ROW_NUMBER() OVER (PARTITION BY owner_id ORDER BY updated_at DESC NULLS LAST) AS rn
  FROM actionbuilder_cleaned.cln_actionbuilder__addresses
  WHERE owner_type = 'Entity'
    AND state IS NOT NULL
),

-- Full enriched entity table: one row per entity
entities_enriched AS (
  SELECT
    e.id                                          AS entity_id,
    e.interact_id,
    e.first_name,
    e.last_name,
    LOWER(TRIM(COALESCE(e.first_name, '')))       AS fn_norm,
    LOWER(TRIM(COALESCE(e.last_name, '')))        AS ln_norm,
    e.created_at,
    pe.email_norm                                 AS email,
    pp.phone_norm                                 AS phone,
    COALESCE(addr.state, '')                      AS state,
    COALESCE(tc.tag_count, 0)                     AS tag_count,
    epi.person_id,
    evb.voterbase_id

  FROM actionbuilder_cleaned.cln_actionbuilder__entities e
  LEFT JOIN primary_emails pe
    ON pe.entity_id = e.id AND pe.email_rank = 1
  LEFT JOIN primary_phones pp
    ON pp.entity_id = e.id AND pp.phone_rank = 1
  LEFT JOIN primary_addresses addr
    ON addr.entity_id = e.id AND addr.rn = 1
  LEFT JOIN tag_counts tc
    ON tc.entity_id = e.id
  LEFT JOIN entity_person_ids epi
    ON epi.entity_id = e.id AND epi.rn = 1
  LEFT JOIN entity_voterbase_ids evb
    ON evb.entity_id = e.id AND evb.rn = 1
),

-- ============================================================
-- Signal 1: voterbase_id_diff_name
-- Same voter file record, but NOT the exact same normalized fn+ln.
-- (Exact-same-name voterbase pairs → dedup_candidates.voterbase_id_match tier)
-- ============================================================
voterbase_pair_ids AS (
  SELECT
    LEAST(ea.interact_id, eb.interact_id)    AS entity_a_iid,
    GREATEST(ea.interact_id, eb.interact_id) AS entity_b_iid,
    ea.voterbase_id                          AS signal_value
  FROM entities_enriched ea
  JOIN entities_enriched eb
    ON ea.voterbase_id = eb.voterbase_id
    AND ea.entity_id < eb.entity_id
    -- Only pairs where full name is NOT exactly equal
    AND NOT (ea.fn_norm = eb.fn_norm AND ea.ln_norm = eb.ln_norm)
  WHERE ea.voterbase_id IS NOT NULL
),

voterbase_signal AS (
  SELECT
    CONCAT(vp.entity_a_iid, ':', vp.entity_b_iid) AS pair_id,
    vp.entity_a_iid   AS entity_a_interact_id,
    ea.first_name     AS entity_a_first_name,
    ea.last_name      AS entity_a_last_name,
    ea.email          AS entity_a_email,
    ea.phone          AS entity_a_phone,
    ea.state          AS entity_a_state,
    ea.tag_count      AS entity_a_tag_count,
    ea.person_id      AS entity_a_person_id,
    ea.voterbase_id   AS entity_a_voterbase_id,
    DATE(ea.created_at) AS entity_a_created_date,
    vp.entity_b_iid   AS entity_b_interact_id,
    eb.first_name     AS entity_b_first_name,
    eb.last_name      AS entity_b_last_name,
    eb.email          AS entity_b_email,
    eb.phone          AS entity_b_phone,
    eb.state          AS entity_b_state,
    eb.tag_count      AS entity_b_tag_count,
    eb.person_id      AS entity_b_person_id,
    eb.voterbase_id   AS entity_b_voterbase_id,
    DATE(eb.created_at) AS entity_b_created_date,
    'voterbase_id_diff_name' AS signal_type,
    vp.signal_value
  FROM voterbase_pair_ids vp
  JOIN entities_enriched ea ON vp.entity_a_iid = ea.interact_id
  JOIN entities_enriched eb ON vp.entity_b_iid = eb.interact_id
),

-- ============================================================
-- Signal 2: shared_phone_same_lastname
-- Same 10-digit phone on any of each entity's phones,
-- same normalized last name, not already caught by voterbase signal.
-- ============================================================
phone_pair_ids AS (
  SELECT
    LEAST(ea.interact_id, eb.interact_id)    AS entity_a_iid,
    GREATEST(ea.interact_id, eb.interact_id) AS entity_b_iid,
    pA.phone_norm                            AS signal_value
  FROM all_ab_phones pA
  JOIN all_ab_phones pB
    ON pA.phone_norm = pB.phone_norm
    AND pA.entity_id < pB.entity_id
    AND LENGTH(pA.phone_norm) = 10
  JOIN entities_enriched ea ON pA.entity_id = ea.entity_id
  JOIN entities_enriched eb ON pB.entity_id = eb.entity_id
  WHERE ea.ln_norm != ''
    AND ea.ln_norm = eb.ln_norm
),

phone_signal AS (
  SELECT
    CONCAT(pp.entity_a_iid, ':', pp.entity_b_iid) AS pair_id,
    pp.entity_a_iid   AS entity_a_interact_id,
    ea.first_name     AS entity_a_first_name,
    ea.last_name      AS entity_a_last_name,
    ea.email          AS entity_a_email,
    ea.phone          AS entity_a_phone,
    ea.state          AS entity_a_state,
    ea.tag_count      AS entity_a_tag_count,
    ea.person_id      AS entity_a_person_id,
    ea.voterbase_id   AS entity_a_voterbase_id,
    DATE(ea.created_at) AS entity_a_created_date,
    pp.entity_b_iid   AS entity_b_interact_id,
    eb.first_name     AS entity_b_first_name,
    eb.last_name      AS entity_b_last_name,
    eb.email          AS entity_b_email,
    eb.phone          AS entity_b_phone,
    eb.state          AS entity_b_state,
    eb.tag_count      AS entity_b_tag_count,
    eb.person_id      AS entity_b_person_id,
    eb.voterbase_id   AS entity_b_voterbase_id,
    DATE(eb.created_at) AS entity_b_created_date,
    'shared_phone_same_lastname' AS signal_type,
    pp.signal_value
  FROM phone_pair_ids pp
  JOIN entities_enriched ea ON pp.entity_a_iid = ea.interact_id
  JOIN entities_enriched eb ON pp.entity_b_iid = eb.interact_id
),

-- ============================================================
-- Combine signals, exclude pairs already handled by dedup_candidates,
-- and deduplicate pairs that appear in both signals.
-- ============================================================
all_signals AS (
  SELECT * FROM voterbase_signal
  UNION ALL
  SELECT * FROM phone_signal
),

-- Entities already in dedup_candidates (either side of any pair)
known_dedup AS (
  SELECT iid FROM (
    SELECT delete_interact_id AS iid FROM {{ ref('dedup_candidates') }}
    UNION ALL
    SELECT keep_interact_id   AS iid FROM {{ ref('dedup_candidates') }}
    WHERE keep_interact_id IS NOT NULL
  )
)

-- Final output: exclude known-dedup pairs, deduplicate cross-signal pairs.
-- If a pair appears in both signals, the voterbase signal takes priority.
SELECT
  sig.pair_id,
  sig.entity_a_interact_id,
  sig.entity_a_first_name,
  sig.entity_a_last_name,
  sig.entity_a_email,
  sig.entity_a_phone,
  sig.entity_a_state,
  sig.entity_a_tag_count,
  sig.entity_a_person_id,
  sig.entity_a_voterbase_id,
  sig.entity_a_created_date,
  sig.entity_b_interact_id,
  sig.entity_b_first_name,
  sig.entity_b_last_name,
  sig.entity_b_email,
  sig.entity_b_phone,
  sig.entity_b_state,
  sig.entity_b_tag_count,
  sig.entity_b_person_id,
  sig.entity_b_voterbase_id,
  sig.entity_b_created_date,
  sig.signal_type,
  sig.signal_value

FROM all_signals sig
LEFT JOIN known_dedup kd1 ON sig.entity_a_interact_id = kd1.iid
LEFT JOIN known_dedup kd2 ON sig.entity_b_interact_id = kd2.iid
WHERE kd1.iid IS NULL AND kd2.iid IS NULL

-- One row per canonical pair; voterbase signal wins if both signals fire
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY sig.pair_id
  ORDER BY CASE sig.signal_type
             WHEN 'voterbase_id_diff_name'      THEN 1
             WHEN 'shared_phone_same_lastname'  THEN 2
           END
) = 1

ORDER BY sig.signal_type, sig.entity_a_last_name, sig.entity_a_first_name
