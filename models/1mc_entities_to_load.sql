-- 1mc_entities_to_load.sql
-- Identifies 1MC prospects (further_action = true) who are not yet in AB
-- and queues them for insertion via insert_new_records.
--
-- Campaign assignment: inherited from the Host who reported the prospect.
-- Name splitting: Airtable stores full name in one field; split on first space.
--
-- Exclusions:
--   - Already in AB (by email match)
--   - Already inserted via sync_log
--   - Missing email (can't match or insert without it)
--   - Missing name (AB API requires first_name)

WITH event_prospects AS (
  SELECT
    friend_family_name as full_name,
    LOWER(TRIM(friend_family_email)) as email_normalized,
    volunteer_name
  FROM `proj-tmc-mem-com.million_conversations.event_reports_attendees`
  WHERE friend_family_email IS NOT NULL
    AND friend_family_name IS NOT NULL
    AND further_action = TRUE
),

conversation_prospects AS (
  SELECT
    friend_family_name as full_name,
    LOWER(TRIM(friend_family_email)) as email_normalized,
    volunteer_name
  FROM `proj-tmc-mem-com.million_conversations.individual_conversation_reports`
  WHERE friend_family_email IS NOT NULL
    AND friend_family_name IS NOT NULL
    AND further_action = TRUE
),

all_prospects AS (
  SELECT * FROM event_prospects
  UNION ALL
  SELECT * FROM conversation_prospects
),

-- Deduplicate by email, keeping the first occurrence
deduped_prospects AS (
  SELECT
    full_name,
    email_normalized,
    volunteer_name,
    ROW_NUMBER() OVER (PARTITION BY email_normalized ORDER BY full_name) as rn
  FROM all_prospects
),

unique_prospects AS (
  SELECT
    full_name,
    email_normalized,
    volunteer_name,
    -- Split name: everything before first space = first_name, rest = last_name
    TRIM(SPLIT(full_name, ' ')[SAFE_OFFSET(0)]) as first_name,
    TRIM(REGEXP_REPLACE(full_name, r'^[^\s]+\s*', '')) as last_name
  FROM deduped_prospects
  WHERE rn = 1
),

-- Find the Host's AB entity and campaigns to assign the prospect to
host_emails AS (
  SELECT
    owner_id as entity_id,
    LOWER(TRIM(email)) as email_normalized
  FROM actionbuilder_cleaned.cln_actionbuilder__emails
  WHERE owner_type = 'Entity'
    AND status IN ('verified', 'user_added')
    AND email IS NOT NULL
),

-- Map volunteer_name to their campaign(s) via Host's AB entity email
-- We need volunteer_email, but attendee rows only have volunteer_name.
-- Join through event_reports and individual_conversation_reports to get host email.
host_campaign_from_events AS (
  SELECT DISTINCT
    era.volunteer_name,
    ce.campaign_id,
    c.interact_id as campaign_interact_id
  FROM `proj-tmc-mem-com.million_conversations.event_reports_attendees` era
  INNER JOIN `proj-tmc-mem-com.million_conversations.event_reports` er
    ON era.volunteer_name = er.volunteer_name
  INNER JOIN host_emails he
    ON LOWER(TRIM(er.volunteer_email)) = he.email_normalized
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns_entities ce
    ON he.entity_id = ce.entity_id
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns c
    ON ce.campaign_id = c.id
  WHERE c.status = 'active'
    AND c.name != 'Test'
    AND era.further_action = TRUE
),

host_campaign_from_conversations AS (
  SELECT DISTINCT
    icr.volunteer_name,
    ce.campaign_id,
    c.interact_id as campaign_interact_id
  FROM `proj-tmc-mem-com.million_conversations.individual_conversation_reports` icr
  INNER JOIN host_emails he
    ON LOWER(TRIM(icr.volunteer_email)) = he.email_normalized
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns_entities ce
    ON he.entity_id = ce.entity_id
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns c
    ON ce.campaign_id = c.id
  WHERE c.status = 'active'
    AND c.name != 'Test'
    AND icr.further_action = TRUE
),

host_campaigns AS (
  SELECT * FROM host_campaign_from_events
  UNION DISTINCT
  SELECT * FROM host_campaign_from_conversations
),

-- Existing AB emails for exclusion
ab_emails AS (
  SELECT LOWER(TRIM(email)) AS email_norm
  FROM actionbuilder_cleaned.cln_actionbuilder__emails
  WHERE owner_type = 'Entity'
    AND email IS NOT NULL
    AND status IN ('verified', 'user_added')
),

-- Already-inserted person_ids from sync_log (for records we can match via identity hub)
already_inserted_person_ids AS (
  SELECT person_id
  FROM `proj-tmc-mem-com`.actionbuilder_sync.sync_log
  WHERE operation = 'insert_entity'
    AND status = 'ok'
    AND person_id IS NOT NULL
),

-- Map prospect emails to person_ids via identity hub (if they exist there)
prospect_person_ids AS (
  SELECT
    LOWER(TRIM(ep.email)) as email_normalized,
    ep.person_id
  FROM core_enhanced.enh_activistpools__emails ep
  WHERE ep.person_id IS NOT NULL
)

SELECT
  up.first_name,
  up.last_name,
  up.email_normalized as email,
  hc.campaign_interact_id,
  up.volunteer_name as referred_by
FROM unique_prospects up
INNER JOIN host_campaigns hc
  ON up.volunteer_name = hc.volunteer_name
LEFT JOIN prospect_person_ids ppi
  ON up.email_normalized = ppi.email_normalized
WHERE up.first_name IS NOT NULL
  AND up.first_name != ''
  -- Not already in AB (by email)
  AND up.email_normalized NOT IN (SELECT email_norm FROM ab_emails)
  -- Not already inserted by a prior run (by person_id if available)
  AND (
    ppi.person_id IS NULL
    OR ppi.person_id NOT IN (SELECT person_id FROM already_inserted_person_ids)
  )

ORDER BY up.last_name, up.first_name
