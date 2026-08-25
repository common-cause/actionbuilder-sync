-- 1mc_participants.sql
-- Identifies people who participated in 1MC conversations or events and
-- tags them as "Participant" in their AB campaigns.
--
-- Sources: event_reports_attendees (friend_family_email) and
-- individual_conversation_reports (friend_family_email).
-- Additive-only: we ADD missing Participant tags, never remove.
-- Output shaped for UNION into updates_to_apply in updates_needed.sql.
--
-- Hardened 2026-08-25 to the pattern 1mc_prospects.sql already proves. Three
-- defects were live until then, all surfaced when 12 weeks of frozen Airtable data
-- landed at once after the upstream airtable-bq-sync repair:
--
--   1. GRAIN IS (entity), NOT (entity, campaign). '1MC Participant' (cat 24) is a
--      UNIVERSAL tag -- see sync.py UNIVERSAL_TAG_IDS_NO_DELETE, "the whole
--      1 Million Conversations section is universal", verified against live AB
--      2026-08-19. One network-level tagging is shared by every campaign and reads
--      back through current_tag_values with campaign_id NULL (5 of its 6 rows on
--      2026-08-25). A per-campaign anti-join can therefore NEVER match it, so every
--      campaign an already-tagged entity belongs to re-emits forever. This is the
--      failure class documented in 1mc_prospects.sql note 3 -- the 219 undrainable
--      OFP rows for campaign 26. Symptom before the fix: 3 entities emitting across
--      2 campaigns each.
--   2. Anti-join keys on the tag INTERACT_ID, not tag_name. This is the Block D
--      lesson (1mc_prospects note 2): names reaching us are stale two ways -- the BQ
--      snapshot holds the pre-rename name until replication catches up, and
--      historical sync_log rows hold whatever name was current when written. This
--      response was itself renamed in Block E (2026-08-18).
--   3. campaigns_entities carries duplicate (entity, campaign) rows -- 795 pairs /
--      798 extra rows on 2026-08-25 -- so the membership CTE must be DISTINCT or
--      each duplicate becomes a second redundant API write. Entities removed from a
--      campaign also linger in that table forever (replication gap #1), so
--      removed_campaign_entities must be subtracted; otherwise the write 404s and,
--      because a 404 creates no tagging, re-emits every night.

{% set tag_participant = '75d26b82-b07c-4092-8e9a-1f979f02a8b6' %}

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
  -- DISTINCT, and interact_ids carried through for the removed-entity anti-join
  -- (removed_campaign_entities is keyed on interact_ids, being built from sync_log).
  -- See header note 3.
  SELECT DISTINCT
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

current_participant_tags AS (
  -- Entity grain, no campaign term, keyed on interact_id. See header notes 1 and 2.
  SELECT DISTINCT
    entity_id
  FROM {{ ref('current_tag_values') }}
  WHERE tag_interact_id = '{{ tag_participant }}'
)

-- Entity+campaign combos that need the Participant tag added
SELECT
  eic.campaign_id,
  pe.entity_id,
  -- Response renamed 2026-08-18 (taxonomy Block E). field_group stays
  -- 'Million Conversations Role' -- it is the AB FIELD name and also the
  -- internal routing token matched in updates_needed.sql.
  '1MC Participant' as field_name,
  'Million Conversations Role' as field_group,
  '1 Million Conversations:|:Million Conversations Role:|:1MC Participant:|:standard_response:1MC Participant' as sync_string,
  '' as current_value,
  '1MC Participant' as correct_value,
  CAST(NULL AS STRING) as removal_ids
FROM participant_entities pe
INNER JOIN entities_in_campaigns eic
  ON pe.entity_id = eic.entity_id

-- Network-level anti-join (no campaign_id term) -- see header note 1. An entity that
-- already holds the tag anywhere needs no write.
LEFT JOIN current_participant_tags cpt
  ON pe.entity_id = cpt.entity_id

-- Entities removed from a campaign still appear in campaigns_entities forever
-- (hard deletes never replicate), so without this the feed emits rows that write
-- as 404 "Entity is not accessible".
LEFT JOIN {{ ref('removed_campaign_entities') }} rem
  ON rem.entity_interact_id = eic.entity_interact_id
 AND rem.campaign_interact_id = eic.campaign_interact_id

WHERE cpt.entity_id IS NULL
  AND rem.entity_interact_id IS NULL
