-- ofp_attendance.sql
-- Identifies ActionBuilder entities who attended Organizing for Power trainings
-- and are missing the corresponding OFP tag.  Output is shaped for direct UNION
-- into updates_to_apply inside updates_needed.sql.
--
-- Multiselect / additive-only: we ADD missing tags, never remove existing ones.

WITH ofp_participations AS (
  -- Mobilize attendees for OFP training timeslots (attended = true only)
  SELECT DISTINCT
    LOWER(TRIM(COALESCE(p.user__email_address, p.email_at_signup))) as email_normalized,
    m.ofp_tag
  FROM mobilize_cleaned.cln_mobilize__participations p
  INNER JOIN {{ ref('ofp_training_map') }} m
    ON p.timeslot_id = m.timeslot_id
  WHERE p.attended = TRUE
    AND COALESCE(p.user__email_address, p.email_at_signup) IS NOT NULL
),

entity_emails AS (
  -- All verified/user_added emails for AB entities
  SELECT
    owner_id as entity_id,
    LOWER(TRIM(email)) as email_normalized
  FROM actionbuilder_cleaned.cln_actionbuilder__emails
  WHERE owner_type = 'Entity'
    AND status IN ('verified', 'user_added')
    AND email IS NOT NULL
),

ofp_entity_tags AS (
  -- Map OFP attendees to AB entities via email
  SELECT DISTINCT
    ee.entity_id,
    op.ofp_tag
  FROM ofp_participations op
  INNER JOIN entity_emails ee
    ON op.email_normalized = ee.email_normalized
),

entities_in_campaigns AS (
  -- All entities in active campaigns. interact_ids are carried through so the
  -- removed-entity anti-join below can match removed_campaign_entities, which is
  -- keyed on interact_ids (it is built from sync_log).
  SELECT
    ce.entity_id,
    ce.campaign_id,
    e.interact_id AS entity_interact_id,
    c.interact_id AS campaign_interact_id
  FROM actionbuilder_cleaned.cln_actionbuilder__campaigns_entities ce
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns c
    ON ce.campaign_id = c.id
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__entities e
    ON e.id = ce.entity_id
  WHERE c.status = 'active'
),

current_ofp_tags AS (
  -- What OFP tags does each entity already have in the NEW universal field?
  --
  -- Match on the universal "Trainings > Organizing For Power" tag interact_ids,
  -- NOT on tag_name. The archived campaign-local "Activism > Organizing For Power"
  -- field shares the same response names ('Organizing Basics', etc.) and its old
  -- tags still read as status=1 in BQ until the archive replicates, plus historical
  -- sync_log rows carry the old interact_ids. Keying on the new universal interact_ids
  -- makes "already has it" detection see only the new field, so existing attendees
  -- get (re)written to the universal field exactly once.
  --
  -- The interact_id is ALSO the canonical source for the name we join on below.
  -- Renames preserve interact_ids but not names, and the names reaching us here are
  -- stale in two ways after a rename: the BQ snapshot keeps the old name until
  -- replication catches up, and historical sync_log rows keep whatever name was
  -- current when they were written. Deriving the name from the interact_id instead
  -- of passing tag_name through keeps the idempotency join stable across renames —
  -- otherwise every existing tagging looks "missing" and gets re-written once.
  -- Names below must match seeds/ofp_training_map.csv exactly.
  --
  -- NOTE THE GRAIN: (entity, tag) — deliberately NOT per campaign. "Organizing For
  -- Power" is a UNIVERSAL field: one network-level tag object per response, shared
  -- across every campaign. A tagging recorded under campaign X is returned by the API
  -- when reading the entity through campaign Y (verified 2026-08-18 against campaign 26:
  -- entities the per-campaign version reported as missing already had every competency).
  -- Grouping by campaign therefore invented work that did not exist — 219 rows for the
  -- Organizing Team campaign that no sync step processes and that could never drain.
  SELECT DISTINCT
    entity_id,
    CASE tag_interact_id
      WHEN 'c06f0496-d59a-4b8f-971e-2aeaea8c8582' THEN 'OFP Training: Organizing Basics'
      WHEN '0e1102dc-bf89-4c06-9ff6-c74d77efc317' THEN 'OFP Training: Storytelling'
      WHEN '282b2017-54a5-41bc-b52c-7863e598950d' THEN 'OFP Training: Relational Organizing'
      WHEN '1ef15001-e59c-4d3d-92fd-7eb001ee9c46' THEN 'OFP Training: Rapid Response Basics'
    END as ofp_tag
  FROM {{ ref('current_tag_values') }}
  WHERE tag_interact_id IN (
    'c06f0496-d59a-4b8f-971e-2aeaea8c8582',  -- OFP Training: Organizing Basics
    '0e1102dc-bf89-4c06-9ff6-c74d77efc317',  -- OFP Training: Storytelling
    '282b2017-54a5-41bc-b52c-7863e598950d',  -- OFP Training: Relational Organizing
    '1ef15001-e59c-4d3d-92fd-7eb001ee9c46'   -- OFP Training: Rapid Response Basics
  )
)

-- Entity+campaign+tag combos that need to be ADDED (not already present)
SELECT
  eic.campaign_id,
  oet.entity_id,
  oet.ofp_tag as field_name,
  'Organizing for Power' as field_group,
  -- Universal field: section "Trainings", field "Organizing For Power" (capital F).
  -- field_group above stays 'Organizing for Power' — it is an internal routing token
  -- consumed by updates_needed's ofp_tag CASE, not sent to the API.
  CONCAT('Trainings:|:Organizing For Power:|:', oet.ofp_tag, ':|:standard_response:', oet.ofp_tag) as sync_string,
  '' as current_value,
  oet.ofp_tag as correct_value,
  CAST(NULL AS STRING) as removal_ids
FROM ofp_entity_tags oet
INNER JOIN entities_in_campaigns eic
  ON oet.entity_id = eic.entity_id

-- Network-level anti-join (no campaign_id term) — see current_ofp_tags above.
-- A competency the entity already holds anywhere is not missing here.
LEFT JOIN current_ofp_tags cot
  ON oet.entity_id = cot.entity_id
  AND oet.ofp_tag = cot.ofp_tag

-- Entities removed from a campaign still appear in campaigns_entities forever
-- (hard deletes never replicate), so without this the feed emits rows that write
-- as 404 "Entity is not accessible".
LEFT JOIN {{ ref('removed_campaign_entities') }} rem
  ON rem.entity_interact_id = eic.entity_interact_id
 AND rem.campaign_interact_id = eic.campaign_interact_id

WHERE cot.entity_id IS NULL
  AND rem.entity_interact_id IS NULL
