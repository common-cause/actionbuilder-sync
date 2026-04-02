-- 1mc_prospects.sql
-- Identifies 1MC Participants who indicated interest in further action
-- (further_action = true) and flags them as Host Prospects in AB.
--
-- Sources: event_reports_attendees and individual_conversation_reports.
-- Additive-only: we ADD missing prospect tags, never remove.
-- Output shaped for UNION into updates_to_apply in updates_needed.sql.
--
-- Note: Leader Prospect is deferred — currently no data distinguishes
-- host vs leader prospect interest. All further_action = true → Host Prospect.

WITH event_prospects AS (
  SELECT DISTINCT
    LOWER(TRIM(friend_family_email)) as email_normalized
  FROM `proj-tmc-mem-com.million_conversations.event_reports_attendees`
  WHERE friend_family_email IS NOT NULL
    AND further_action = TRUE
),

conversation_prospects AS (
  SELECT DISTINCT
    LOWER(TRIM(friend_family_email)) as email_normalized
  FROM `proj-tmc-mem-com.million_conversations.individual_conversation_reports`
  WHERE friend_family_email IS NOT NULL
    AND further_action = TRUE
),

all_prospects AS (
  SELECT email_normalized FROM event_prospects
  UNION DISTINCT
  SELECT email_normalized FROM conversation_prospects
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

prospect_entities AS (
  SELECT DISTINCT
    ee.entity_id
  FROM all_prospects ap
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

current_prospect_tags AS (
  SELECT
    entity_id,
    campaign_id,
    tag_name
  FROM {{ ref('current_tag_values') }}
  WHERE tag_name = 'Host Prospect'
)

SELECT
  eic.campaign_id,
  pe.entity_id,
  'Host Prospect' as field_name,
  'Million Conversations Prospect' as field_group,
  '1 Million Conversations:|:Million Conversations Prospect:|:Host Prospect:|:standard_response:Host Prospect' as sync_string,
  '' as current_value,
  'Host Prospect' as correct_value,
  CAST(NULL AS STRING) as removal_ids
FROM prospect_entities pe
INNER JOIN entities_in_campaigns eic
  ON pe.entity_id = eic.entity_id
LEFT JOIN current_prospect_tags cpt
  ON pe.entity_id = cpt.entity_id
  AND eic.campaign_id = cpt.campaign_id
WHERE cpt.entity_id IS NULL
