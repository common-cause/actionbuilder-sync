-- ofp_universe.sql
-- Canonical person-level source of Organizing for Power (OFP) training attendees,
-- derived purely from observed Mobilize attendance (event 907019 timeslots via the
-- ofp_training_map seed, attended = TRUE).
--
-- Grain: one row per (email_normalized, OFP competency attended).
-- Each row carries best-available contact info plus a ready-to-use universal-field
-- sync string. Consumers:
--   - master_load_qualifiers (ofp_qualifiers CTE) — DISTINCT person, makes OFP a load qualifier
--   - organizing_team_connects  — connect existing AB entities to campaign 26
--   - organizing_team_inserts   — insert stateless attendees directly into campaign 26
--   - organizing_team_review    — surface unclassifiable cases
--
-- Contact info: a person can attend with slightly different contact details across
-- timeslots; we keep the most-recent non-null value per field. person_id is resolved
-- via the identity hub (email) so downstream feeds can anti-join robustly.

WITH ofp_participations AS (
  SELECT
    LOWER(TRIM(COALESCE(p.user__email_address, p.email_at_signup))) AS email_normalized,
    NULLIF(TRIM(p.user__given_name), '')   AS first_name,
    NULLIF(TRIM(p.user__family_name), '')  AS last_name,
    NULLIF(TRIM(p.user__phone_number), '') AS phone_number,
    NULLIF(TRIM(p.user__postal_code), '')  AS zip_code,
    p.utc_created_date                     AS created_at,
    m.ofp_tag                              AS competency
  FROM mobilize_cleaned.cln_mobilize__participations p
  INNER JOIN {{ ref('ofp_training_map') }} m
    ON p.timeslot_id = m.timeslot_id
  WHERE p.attended = TRUE
    AND COALESCE(p.user__email_address, p.email_at_signup) IS NOT NULL
),

-- Best (most-recent non-null) contact info per person
contacts AS (
  SELECT
    email_normalized,
    ARRAY_AGG(first_name   IGNORE NULLS ORDER BY created_at DESC)[SAFE_OFFSET(0)] AS first_name,
    ARRAY_AGG(last_name    IGNORE NULLS ORDER BY created_at DESC)[SAFE_OFFSET(0)] AS last_name,
    ARRAY_AGG(phone_number IGNORE NULLS ORDER BY created_at DESC)[SAFE_OFFSET(0)] AS phone_number,
    ARRAY_AGG(zip_code     IGNORE NULLS ORDER BY created_at DESC)[SAFE_OFFSET(0)] AS zip_code
  FROM ofp_participations
  GROUP BY email_normalized
),

-- One person_id per email via the identity hub (used for robust downstream anti-joins)
person_map AS (
  SELECT
    LOWER(TRIM(email)) AS email_normalized,
    MIN(person_id)     AS person_id
  FROM core_enhanced.enh_activistpools__emails
  WHERE person_id IS NOT NULL
    AND email IS NOT NULL
  GROUP BY LOWER(TRIM(email))
),

-- Distinct competencies attended per person
competencies AS (
  SELECT DISTINCT email_normalized, competency
  FROM ofp_participations
),

-- Authoritative zip -> state crosswalk (one state per zip; ~38.5k zips).
-- Used to route attendees to their state campaign and to stamp region on inserts,
-- since the voter-file state fallback misses people not matched to the voter file.
zip_state AS (
  SELECT vb_tsmart_zip AS zip5, vb_tsmart_state AS state
  FROM geo_crosswalks_cleaned.cln_geo_crosswalks__zip_county_lookup
  WHERE vb_tsmart_zip IS NOT NULL
)

SELECT
  comp.email_normalized,
  pm.person_id,
  c.first_name,
  c.last_name,
  c.phone_number,
  c.zip_code,
  zs.state,
  comp.competency,
  -- Universal field: section "Trainings", field "Organizing For Power" (capital F).
  -- standard_response multiselect; value is the response name itself.
  CONCAT('Trainings:|:Organizing For Power:|:', comp.competency, ':|:standard_response:', comp.competency)
    AS sync_string
FROM competencies comp
JOIN contacts c USING (email_normalized)
LEFT JOIN person_map pm USING (email_normalized)
LEFT JOIN zip_state zs
  ON zs.zip5 = SUBSTR(REGEXP_REPLACE(COALESCE(c.zip_code, ''), r'[^0-9]', ''), 1, 5)
