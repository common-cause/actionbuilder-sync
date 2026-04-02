-- 1mc_role_attendance.sql
-- Identifies ActionBuilder entities who completed 1MC trainings (Leader or Host)
-- and are missing the corresponding Campaign Role tag.  Output is shaped for
-- direct UNION into updates_to_apply inside updates_needed.sql.
--
-- Multiselect / additive-only: we ADD missing tags, never remove existing ones.
-- Same pattern as ofp_attendance.sql.

WITH training_participations AS (
  -- Mobilize attendees for 1MC training timeslots (attended = true only)
  SELECT DISTINCT
    LOWER(TRIM(COALESCE(p.user__email_address, p.email_at_signup))) as email_normalized,
    m.role_tag
  FROM mobilize_cleaned.cln_mobilize__participations p
  INNER JOIN {{ ref('1mc_training_map') }} m
    ON p.timeslot_id = CAST(m.timeslot_id AS INT64)
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

role_entity_tags AS (
  -- Map training attendees to AB entities via email
  SELECT DISTINCT
    ee.entity_id,
    tp.role_tag
  FROM training_participations tp
  INNER JOIN entity_emails ee
    ON tp.email_normalized = ee.email_normalized
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

current_role_tags AS (
  -- What 1MC Campaign Role tags does each entity already have?
  SELECT
    entity_id,
    campaign_id,
    tag_name as role_tag
  FROM {{ ref('current_tag_values') }}
  WHERE tag_name IN ('Leader', 'Host')
)

-- Entity+campaign+tag combos that need to be ADDED (not already present)
SELECT
  eic.campaign_id,
  ret.entity_id,
  ret.role_tag as field_name,
  'Million Conversations Role' as field_group,
  CONCAT('1 Million Conversations:|:Million Conversations Role:|:', ret.role_tag, ':|:standard_response:', ret.role_tag) as sync_string,
  '' as current_value,
  ret.role_tag as correct_value,
  CAST(NULL AS STRING) as removal_ids
FROM role_entity_tags ret
INNER JOIN entities_in_campaigns eic
  ON ret.entity_id = eic.entity_id
LEFT JOIN current_role_tags crt
  ON ret.entity_id = crt.entity_id
  AND eic.campaign_id = crt.campaign_id
  AND ret.role_tag = crt.role_tag
WHERE crt.entity_id IS NULL
