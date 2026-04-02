-- 1mc_participants.sql
-- Identifies people who participated in 1MC conversations or events and
-- tags them as "Participant" in their AB campaigns.
--
-- Sources: event_reports_attendees (friend_family_email) and
-- individual_conversation_reports (friend_family_email).
-- Additive-only: we ADD missing Participant tags, never remove.
-- Output shaped for UNION into updates_to_apply in updates_needed.sql.

WITH event_participants AS (
  SELECT DISTINCT
    LOWER(TRIM(friend_family_email)) as email_normalized
  FROM `proj-tmc-mem-com.million_conversations.event_reports_attendees`
  WHERE friend_family_email IS NOT NULL
),

conversation_participants AS (
  SELECT DISTINCT
    LOWER(TRIM(friend_family_email)) as email_normalized
  FROM `proj-tmc-mem-com.million_conversations.individual_conversation_reports`
  WHERE friend_family_email IS NOT NULL
),

all_participants AS (
  SELECT email_normalized FROM event_participants
  UNION DISTINCT
  SELECT email_normalized FROM conversation_participants
),

entity_emails AS (
  SELECT
    owner_id as entity_id,
    LOWER(TRIM(email)) as email_normalized
  FROM actionbuilder_cleaned.cln_actionbuilder__emails
  WHERE owner_type = 'Entity'
    AND status IN ('verified', 'user_added')
    AND email IS NOT NULL
),

participant_entities AS (
  SELECT DISTINCT
    ee.entity_id
  FROM all_participants ap
  INNER JOIN entity_emails ee
    ON ap.email_normalized = ee.email_normalized
),

entities_in_campaigns AS (
  SELECT
    ce.entity_id,
    ce.campaign_id
  FROM actionbuilder_cleaned.cln_actionbuilder__campaigns_entities ce
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns c
    ON ce.campaign_id = c.id
  WHERE c.status = 'active'
),

current_participant_tags AS (
  SELECT
    entity_id,
    campaign_id,
    tag_name
  FROM {{ ref('current_tag_values') }}
  WHERE tag_name = 'Participant'
)

-- Entity+campaign combos that need the Participant tag added
SELECT
  eic.campaign_id,
  pe.entity_id,
  'Participant' as field_name,
  'Million Conversations Role' as field_group,
  '1 Million Conversations:|:Million Conversations Role:|:Participant:|:standard_response:Participant' as sync_string,
  '' as current_value,
  'Participant' as correct_value,
  CAST(NULL AS STRING) as removal_ids
FROM participant_entities pe
INNER JOIN entities_in_campaigns eic
  ON pe.entity_id = eic.entity_id
LEFT JOIN current_participant_tags cpt
  ON pe.entity_id = cpt.entity_id
  AND eic.campaign_id = cpt.campaign_id
WHERE cpt.entity_id IS NULL
