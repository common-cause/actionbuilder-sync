-- 1mc_notes.sql
-- Formats Airtable text fields into note payloads for append_note API calls.
--
-- Three source tables map to three response names:
--   event_reports              → "Event Host Notes"        (on Host's record)
--   event_reports_attendees    → "Event Attendee Notes"    (on Attendee's record)
--   individual_conversation_reports → "Conversation Host Notes" (on Host's record)
--
-- Each Airtable record produces at most one note, concatenating all non-null
-- text fields with labels. Idempotency: filter out _airtable_record_id values
-- already logged in sync_log with operation = 'append_note'.
--
-- Output columns match what the sync script needs to call ab.append_note().

WITH entity_emails AS (
  SELECT
    owner_id as entity_id,
    LOWER(TRIM(email)) as email_normalized
  FROM actionbuilder_cleaned.cln_actionbuilder__emails
  WHERE owner_type = 'Entity'
    AND status IN ('verified', 'user_added')
    AND email IS NOT NULL
),

entity_interact_ids AS (
  SELECT id as entity_id, interact_id as entity_interact_id
  FROM actionbuilder_cleaned.cln_actionbuilder__entities
),

entities_in_campaigns AS (
  SELECT
    ce.entity_id,
    ce.campaign_id,
    c.interact_id as campaign_interact_id
  FROM actionbuilder_cleaned.cln_actionbuilder__campaigns_entities ce
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns c
    ON ce.campaign_id = c.id
  WHERE c.status = 'active'
    AND c.name != 'Test'
),

-- Already-appended notes (by airtable record id + response name)
already_appended AS (
  SELECT DISTINCT tag_name as airtable_record_key
  FROM `proj-tmc-mem-com`.actionbuilder_sync.sync_log
  WHERE operation = 'append_note'
    AND status = 'ok'
),

-- ============================================================
-- Event Host Notes: from event_reports, attached to Host
-- ============================================================
event_host_notes AS (
  SELECT
    er._airtable_record_id,
    LOWER(TRIM(er.volunteer_email)) as email_normalized,
    'Event Host Notes' as response_name,
    CONCAT(
      COALESCE(CONCAT('Event: ', er.event_name, ', '), ''),
      COALESCE(CONCAT(FORMAT_TIMESTAMP('%Y-%m-%d', er.event_date), ', '), ''),
      COALESCE(er.event_city, ''),
      CASE
        WHEN er.resonant_values_or_issues IS NOT NULL
        THEN CONCAT('\nResonant values/issues: ', er.resonant_values_or_issues)
        ELSE ''
      END,
      CASE
        WHEN er.description_and_insights IS NOT NULL
        THEN CONCAT('\nDescription: ', er.description_and_insights)
        ELSE ''
      END,
      CASE
        WHEN er.other_notes_or_followup IS NOT NULL
        THEN CONCAT('\nFollow-up: ', er.other_notes_or_followup)
        ELSE ''
      END
    ) as note_body
  FROM `proj-tmc-mem-com.million_conversations.event_reports` er
  WHERE er.volunteer_email IS NOT NULL
    AND (er.resonant_values_or_issues IS NOT NULL
      OR er.description_and_insights IS NOT NULL
      OR er.other_notes_or_followup IS NOT NULL)
),

-- ============================================================
-- Event Attendee Notes: from event_reports_attendees, attached to Attendee
-- ============================================================
event_attendee_notes AS (
  SELECT
    era._airtable_record_id,
    LOWER(TRIM(era.friend_family_email)) as email_normalized,
    'Event Attendee Notes' as response_name,
    CONCAT(
      COALESCE(CONCAT('Event at ', era.event_location, ', '), ''),
      COALESCE(CONCAT(FORMAT_TIMESTAMP('%Y-%m-%d', era.event_date), '. '), ''),
      'Host: ', COALESCE(era.volunteer_name, 'Unknown'),
      CASE
        WHEN era.free_form_response IS NOT NULL
        THEN CONCAT('\nResponse: ', era.free_form_response)
        ELSE ''
      END,
      CASE
        WHEN era.other_notes_or_followup IS NOT NULL
        THEN CONCAT('\nHost notes: ', era.other_notes_or_followup)
        ELSE ''
      END
    ) as note_body
  FROM `proj-tmc-mem-com.million_conversations.event_reports_attendees` era
  WHERE era.friend_family_email IS NOT NULL
    AND (era.free_form_response IS NOT NULL
      OR era.other_notes_or_followup IS NOT NULL)
),

-- ============================================================
-- Conversation Host Notes: from individual_conversation_reports, attached to Host
-- ============================================================
conversation_host_notes AS (
  SELECT
    icr._airtable_record_id,
    LOWER(TRIM(icr.volunteer_email)) as email_normalized,
    'Conversation Host Notes' as response_name,
    CONCAT(
      'Conversation with ', COALESCE(icr.friend_family_name, 'Unknown'),
      COALESCE(CONCAT(', ', icr.conversation_location), ''),
      COALESCE(CONCAT(', ', FORMAT_TIMESTAMP('%Y-%m-%d', icr.conversation_date)), ''),
      CASE
        WHEN icr.resonant_values_or_issues IS NOT NULL
        THEN CONCAT('\nResonant values/issues: ', icr.resonant_values_or_issues)
        ELSE ''
      END,
      CASE
        WHEN icr.description_and_insights IS NOT NULL
        THEN CONCAT('\nDescription: ', icr.description_and_insights)
        ELSE ''
      END,
      CASE
        WHEN icr.other_notes_or_followup IS NOT NULL
        THEN CONCAT('\nFollow-up: ', icr.other_notes_or_followup)
        ELSE ''
      END
    ) as note_body
  FROM `proj-tmc-mem-com.million_conversations.individual_conversation_reports` icr
  WHERE icr.volunteer_email IS NOT NULL
    AND (icr.resonant_values_or_issues IS NOT NULL
      OR icr.description_and_insights IS NOT NULL
      OR icr.other_notes_or_followup IS NOT NULL)
),

-- ============================================================
-- Combine all note sources
-- ============================================================
all_notes AS (
  SELECT * FROM event_host_notes
  UNION ALL
  SELECT * FROM event_attendee_notes
  UNION ALL
  SELECT * FROM conversation_host_notes
),

-- Match to AB entities and campaigns
notes_with_entities AS (
  SELECT DISTINCT
    an._airtable_record_id,
    an.response_name,
    an.note_body,
    ee.entity_id,
    eic.campaign_interact_id
  FROM all_notes an
  INNER JOIN entity_emails ee
    ON an.email_normalized = ee.email_normalized
  INNER JOIN entities_in_campaigns eic
    ON ee.entity_id = eic.entity_id
)

SELECT
  nwe._airtable_record_id,
  eii.entity_interact_id,
  nwe.campaign_interact_id,
  '1 Million Conversations' as section,
  'Conversation Notes' as field,
  nwe.response_name,
  nwe.note_body
FROM notes_with_entities nwe
INNER JOIN entity_interact_ids eii
  ON nwe.entity_id = eii.entity_id
-- Idempotency: skip records already appended
WHERE CONCAT(nwe._airtable_record_id, ':', nwe.response_name)
  NOT IN (SELECT airtable_record_key FROM already_appended)
ORDER BY nwe._airtable_record_id
