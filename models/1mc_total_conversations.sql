-- 1mc_total_conversations.sql
-- Computes Total Conversations per Host from Airtable event reports and
-- individual conversation reports.  Compares against current AB tag values
-- and outputs rows needing update.
--
-- Total Conversations = SUM(event attendee_count) + COUNT(individual conversations)
-- Output shaped for UNION into updates_to_apply in updates_needed.sql.
--
-- Hardened 2026-08-25 to the pattern 1mc_prospects.sql already proves; the full
-- write-up lives in 1mc_participants.sql's header. Applied here:
--
--   1. GRAIN IS (entity), NOT (entity, campaign). '1MC Total Conversations'
--      (cat 26) is UNIVERSAL and SINGLE-select -- sync.py
--      UNIVERSAL_TAG_IDS_NO_DELETE, verified against live AB 2026-08-19. The
--      tagging reads back with campaign_id NULL, so the old per-campaign join
--      matched nothing and compared the real total against the COALESCE default of
--      '0' in every campaign but the one the last write happened to go under --
--      re-emitting forever (1mc_prospects note 3, the 219 undrainable OFP rows).
--   2. Keyed on tag INTERACT_ID, not tag_name (Block D lesson; this response was
--      renamed in Block E).
--   3. campaigns_entities is DISTINCT-ed (795 duplicate pairs on 2026-08-25) and
--      removed_campaign_entities is subtracted (replication gap #1).
--   4. removal_ids is now an explicit NULL. updates_needed already hardcodes
--      million_conversations_activity_tag_remove to NULL -- universal taggings are
--      API-undeletable and single-select replaces on write -- so carrying a live
--      removal_string here was dead weight that invited its reintroduction.

{% set tag_total_conversations = '0aa3763d-18de-496f-8466-22e156f6a162' %}

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
  -- Map host totals to AB entities via email and SUM across the entity's addresses.
  -- (The comment here used to say "take max if multiple emails match", which the
  -- code has never done. SUM is the intended behaviour -- a host who reported under
  -- two of their own addresses did both sets of conversations -- but it does mean a
  -- person whose two addresses cover the SAME Airtable reports would be counted
  -- twice. No such case exists today; revisit if hosts start double-reporting.)
  SELECT
    ee.entity_id,
    SUM(ht.total_conversations) as total_conversations
  FROM host_totals ht
  INNER JOIN entity_emails ee
    ON ht.email_normalized = ee.email_normalized
  GROUP BY ee.entity_id
),

entities_in_campaigns AS (
  -- DISTINCT, and interact_ids carried through for the removed-entity anti-join.
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

current_values AS (
  -- Entity grain, keyed on interact_id, freshest row wins. Being universal
  -- single-select there is exactly ONE true current value per entity, but the
  -- overlay can expose it twice: the sync_log-derived row carries the campaign the
  -- write went under, while the BQ snapshot row carries campaign_id NULL. Ordering
  -- by tag_applied_at takes the fresher of the two. (Ordering by the value itself
  -- would be wrong twice over -- these are numbers held as strings, so MAX is
  -- lexical, and a total can legitimately go DOWN when an Airtable record is
  -- deleted, which is exactly what happened to event_reports_attendees in the
  -- 2026-06/08 outage.)
  SELECT
    entity_id,
    current_value
  FROM (
    SELECT
      entity_id,
      current_value,
      ROW_NUMBER() OVER (
        PARTITION BY entity_id
        ORDER BY tag_applied_at DESC
      ) as rn
    FROM {{ ref('current_tag_values') }}
    WHERE tag_interact_id = '{{ tag_total_conversations }}'
  )
  WHERE rn = 1
)

SELECT
  eic.campaign_id,
  het.entity_id,
  -- Renamed 2026-08-18 (taxonomy Block E): only the RESPONSE (tag 86) became
  -- '1MC Total Conversations'. The AB field name -- sync-string segment 2 --
  -- is still 'Total Conversations', and field_group is the internal routing
  -- token matched in updates_needed.sql. Edit these positionally, never by
  -- find/replace.
  '1MC Total Conversations' as field_name,
  'Total Conversations' as field_group,
  CONCAT('1 Million Conversations:|:Total Conversations:|:1MC Total Conversations:|:number_response:', CAST(het.total_conversations AS STRING)) as sync_string,
  COALESCE(cv.current_value, '0') as current_value,
  CAST(het.total_conversations AS STRING) as correct_value,
  -- Universal single-select: replace-on-write, and universal taggings cannot be
  -- deleted through the API. Never emit a removal here. See header note 4.
  CAST(NULL AS STRING) as removal_ids
FROM host_entity_totals het
INNER JOIN entities_in_campaigns eic
  ON het.entity_id = eic.entity_id

-- Network-level join (no campaign_id term) -- see header note 1.
LEFT JOIN current_values cv
  ON het.entity_id = cv.entity_id

-- Entities removed from a campaign still appear in campaigns_entities forever
-- (hard deletes never replicate); without this the write 404s and re-emits nightly.
LEFT JOIN {{ ref('removed_campaign_entities') }} rem
  ON rem.entity_interact_id = eic.entity_interact_id
 AND rem.campaign_interact_id = eic.campaign_interact_id

WHERE CAST(het.total_conversations AS STRING) != COALESCE(cv.current_value, '0')
  AND rem.entity_interact_id IS NULL
