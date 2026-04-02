-- 1mc_total_conversations.sql
-- Computes Total Conversations per Host from Airtable event reports and
-- individual conversation reports.  Compares against current AB tag values
-- and outputs rows needing update.
--
-- Total Conversations = SUM(event attendee_count) + COUNT(individual conversations)
-- Output shaped for UNION into updates_to_apply in updates_needed.sql.

WITH event_conversations AS (
  -- Each event report contributes its attendee_count toward the host's total
  SELECT
    LOWER(TRIM(volunteer_email)) as email_normalized,
    CAST(attendee_count AS INT64) as conversations
  FROM `proj-tmc-mem-com.million_conversations.event_reports`
  WHERE volunteer_email IS NOT NULL
    AND attendee_count IS NOT NULL
),

individual_conversations AS (
  -- Each individual conversation report counts as 1
  SELECT
    LOWER(TRIM(volunteer_email)) as email_normalized,
    1 as conversations
  FROM `proj-tmc-mem-com.million_conversations.individual_conversation_reports`
  WHERE volunteer_email IS NOT NULL
),

host_totals AS (
  SELECT
    email_normalized,
    SUM(conversations) as total_conversations
  FROM (
    SELECT * FROM event_conversations
    UNION ALL
    SELECT * FROM individual_conversations
  )
  GROUP BY email_normalized
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

host_entity_totals AS (
  -- Map host totals to AB entities via email; take max if multiple emails match
  SELECT
    ee.entity_id,
    SUM(ht.total_conversations) as total_conversations
  FROM host_totals ht
  INNER JOIN entity_emails ee
    ON ht.email_normalized = ee.email_normalized
  GROUP BY ee.entity_id
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

current_values AS (
  SELECT
    entity_id,
    campaign_id,
    current_value,
    removal_string
  FROM {{ ref('current_tag_values') }}
  WHERE tag_name = 'Total Conversations'
)

SELECT
  eic.campaign_id,
  het.entity_id,
  'Total Conversations' as field_name,
  'Total Conversations' as field_group,
  CONCAT('1 Million Conversations:|:Total Conversations:|:Total Conversations:|:number_response:', CAST(het.total_conversations AS STRING)) as sync_string,
  COALESCE(cv.current_value, '0') as current_value,
  CAST(het.total_conversations AS STRING) as correct_value,
  cv.removal_string as removal_ids
FROM host_entity_totals het
INNER JOIN entities_in_campaigns eic
  ON het.entity_id = eic.entity_id
LEFT JOIN current_values cv
  ON het.entity_id = cv.entity_id
  AND eic.campaign_id = cv.campaign_id
WHERE CAST(het.total_conversations AS STRING) != COALESCE(cv.current_value, '0')
