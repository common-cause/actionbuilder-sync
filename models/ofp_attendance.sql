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
  -- All entities in active campaigns
  SELECT
    ce.entity_id,
    ce.campaign_id
  FROM actionbuilder_cleaned.cln_actionbuilder__campaigns_entities ce
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns c
    ON ce.campaign_id = c.id
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
  SELECT
    entity_id,
    campaign_id,
    tag_name as ofp_tag
  FROM {{ ref('current_tag_values') }}
  WHERE tag_interact_id IN (
    'c06f0496-d59a-4b8f-971e-2aeaea8c8582',  -- Organizing Basics
    '0e1102dc-bf89-4c06-9ff6-c74d77efc317',  -- Storytelling
    '282b2017-54a5-41bc-b52c-7863e598950d',  -- Relational Organizing
    '1ef15001-e59c-4d3d-92fd-7eb001ee9c46'   -- Rapid Response Basics
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
LEFT JOIN current_ofp_tags cot
  ON oet.entity_id = cot.entity_id
  AND eic.campaign_id = cot.campaign_id
  AND oet.ofp_tag = cot.ofp_tag
WHERE cot.entity_id IS NULL
