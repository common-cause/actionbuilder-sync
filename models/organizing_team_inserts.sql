-- organizing_team_inserts.sql
-- OFP attendees who are NOT in ActionBuilder and have NO state-load path, inserted
-- directly into the Organizing Team campaign (id 26) with only the universal OFP
-- competencies set (no campaign-local participation fields).
--
-- Who lands here:
--   - in AB already                -> handled by organizing_team_connects (excluded here)
--   - not in AB, staffed state      -> loaded into their state campaign by the regular
--                                      insert flow (they qualify via ofp_qualifiers in
--                                      master_load_qualifiers, so they appear in
--                                      deduplicated_names_to_load) -> excluded here
--   - not in AB, unstaffed/NULL     -> dropped by deduplicated_names_to_load's state
--                                      filter, so NOT in that feed -> included here
--
-- We therefore take OFP attendees who are (a) not in AB and (b) not in
-- deduplicated_names_to_load, then re-apply the insert guards so junk (test accounts,
-- nameless records) never reaches campaign 26. Anti-joins use BOTH person_id and
-- normalized email for robustness against email differences across feeds.
--
-- Grain: one row per (person, competency); sync.py insert_organizing_team groups by
-- person to build one entity with all its OFP tags.

{% set campaign_26 = '1e7e58fd-efb4-4810-91dc-2e7aac08625a' %}

WITH ab_emails AS (
  SELECT DISTINCT LOWER(TRIM(email)) AS email_norm
  FROM actionbuilder_cleaned.cln_actionbuilder__emails
  WHERE owner_type = 'Entity'
    AND email IS NOT NULL
    AND status IN ('verified', 'user_added')
),

ab_person_ids AS (
  SELECT DISTINCT ep.person_id
  FROM actionbuilder_cleaned.cln_actionbuilder__emails abe
  INNER JOIN core_enhanced.enh_activistpools__emails ep
    ON LOWER(TRIM(abe.email)) = LOWER(TRIM(ep.email))
  WHERE abe.owner_type = 'Entity'
    AND abe.email IS NOT NULL
    AND ep.person_id IS NOT NULL
),

-- People the regular sync will load into a state campaign (so not ours to insert into 26)
state_loadable AS (
  SELECT DISTINCT
    person_id,
    LOWER(TRIM(email)) AS email_norm
  FROM {{ ref('deduplicated_names_to_load') }}
),

-- States that have an active AB campaign. Anyone whose (zip-derived) state is staffed
-- has a state-load path and must NOT be inserted directly into campaign 26 — even if they
-- slipped out of deduplicated_names_to_load (e.g. collapsed by its gmail-canonical / held-out
-- dedup). They reach 26 via the connect feed once their state entity exists. This makes the
-- 26-insert feed strictly "no state campaign", independent of deduplicated's internal dedup.
staffed_states AS (
  SELECT s.abbreviation AS state
  FROM actionnetwork_views.states s
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns c
    ON c.name = s.name
  WHERE c.status = 'active'
    AND c.name != 'Test'
  UNION DISTINCT SELECT 'DC'
  UNION DISTINCT SELECT 'VA'
),

-- Person-level OFP attendees (collapse the per-competency grain)
ofp_people AS (
  SELECT
    email_normalized,
    ANY_VALUE(person_id)   AS person_id,
    ANY_VALUE(first_name)  AS first_name,
    ANY_VALUE(last_name)   AS last_name,
    ANY_VALUE(phone_number) AS phone_number,
    ANY_VALUE(zip_code)    AS zip_code,
    ANY_VALUE(state)       AS state
  FROM {{ ref('ofp_universe') }}
  GROUP BY email_normalized
),

eligible AS (
  SELECT op.*
  FROM ofp_people op

  -- Not already in AB (by email or person_id)
  LEFT JOIN ab_emails ae      ON ae.email_norm = op.email_normalized
  LEFT JOIN ab_person_ids api ON api.person_id = op.person_id

  -- Not going to be loaded into a state campaign (by email or person_id)
  LEFT JOIN state_loadable sl_e ON sl_e.email_norm = op.email_normalized
  LEFT JOIN state_loadable sl_p ON sl_p.person_id  = op.person_id

  WHERE ae.email_norm IS NULL
    AND api.person_id IS NULL
    AND sl_e.email_norm IS NULL
    AND sl_p.person_id IS NULL

    -- Strictly "no state campaign": exclude anyone whose zip-derived state is staffed
    AND (op.state IS NULL OR op.state NOT IN (SELECT state FROM staffed_states))

    -- Insert guards (mirror deduplicated_names_to_load)
    AND op.first_name IS NOT NULL
    AND NOT REGEXP_CONTAINS(op.email_normalized, r'^[^+]+\+[^@]+@gmail\.com$')
)

SELECT
  '{{ campaign_26 }}'  AS campaign_interact_id,
  e.email_normalized,
  e.person_id,
  e.first_name,
  e.last_name,
  e.phone_number,
  e.state,   -- zip-derived; real state even when it has no CC campaign (stamped as region)
  e.zip_code,
  u.competency          AS field_name,
  u.sync_string
FROM eligible e
JOIN {{ ref('ofp_universe') }} u ON u.email_normalized = e.email_normalized
ORDER BY e.email_normalized, u.competency
