-- 1mc_prospects.sql
-- Identifies 1MC Participants who indicated interest in further action
-- (further_action = true) and sets their status in the 1MC Prospect Status field.
--
-- Sources: event_reports_attendees and individual_conversation_reports.
-- Output shaped for UNION into updates_to_apply in updates_needed.sql.
--
-- Taxonomy Block G (2026-08-19) rewrote this model. It used to write the
-- multi-select `1 Million Conversations > Million Conversations Prospect >
-- Host Prospect` (cat 25, archived in Block H). It now writes the SINGLE-SELECT
-- UNIVERSAL field `1MC Prospect Status` (cat 31). Three consequences, all load-bearing:
--
--   1. NEVER emit removals. Universal taggings are API-undeletable (a delete
--      returns 404), and single-select replaces on write anyway. "Cleared" is
--      expressed as an explicit `1MC Prospect: None` write, not as a deletion.
--   2. Idempotency keys on tag INTERACT_IDS, not names. This is the Block D
--      lesson: `ofp_attendance` filtered on interact_id but JOINED on tag_name,
--      so the Block D rename silently unmatched 1,452 of 1,452 rows and the feed
--      re-emitted everything nightly. Names reaching us here are stale two ways
--      -- the BQ snapshot keeps the pre-rename name until replication catches up,
--      and historical sync_log rows keep whatever name was current when written.
--      The interact_id is the only stable key, so it is also the source of the
--      name we compare on.
--   3. GRAIN IS (entity), NOT (entity, campaign). The field is universal: one
--      network-level tag object shared by every campaign, and a tagging written
--      under campaign X reads back through campaign Y. Grouping the "already has
--      it" check by campaign invents work that can never drain (this is what
--      produced 219 undrainable OFP rows for campaign 26).
--
-- Leader Prospect stays deferred -- no data distinguishes host- from
-- leader-prospect interest, so every further_action = true is a Host Prospect.
-- The value is wired up here so a future source only needs a qualifier branch.

{% set tag_host_prospect   = '7c946169-3f88-4c45-9bf9-0a8bb5a2e12c' %}
{% set tag_leader_prospect = '644a722f-a3f9-4e80-aecb-3bedea895f73' %}
{% set tag_prospect_none   = '0825f28d-adf8-4f2d-9244-56288190570e' %}

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

-- Entities that currently qualify as a prospect, with the status they should hold
qualifying_entities AS (
  SELECT DISTINCT
    ee.entity_id,
    '1MC Host Prospect' as target_status
  FROM all_prospects ap
  INNER JOIN entity_emails ee
    ON ap.email_normalized = ee.email_normalized
),

entities_in_campaigns AS (
  -- interact_ids are carried through so the removed-entity anti-join can match
  -- removed_campaign_entities, which is keyed on interact_ids (built from sync_log).
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

-- Current status per entity in the NEW field, derived from interact_id (see note 2).
-- Network-level grain, no campaign term (see note 3).
current_status AS (
  SELECT DISTINCT
    entity_id,
    CASE tag_interact_id
      WHEN '{{ tag_host_prospect }}'   THEN '1MC Host Prospect'
      WHEN '{{ tag_leader_prospect }}' THEN '1MC Leader Prospect'
      WHEN '{{ tag_prospect_none }}'   THEN '1MC Prospect: None'
    END as held_status
  FROM {{ ref('current_tag_values') }}
  WHERE tag_interact_id IN (
    '{{ tag_host_prospect }}',    -- 1MC Host Prospect
    '{{ tag_leader_prospect }}',  -- 1MC Leader Prospect
    '{{ tag_prospect_none }}'     -- 1MC Prospect: None
  )
),

-- What each entity should hold: its qualifying status, or an explicit None if it
-- once held a real prospect status and no longer qualifies. Entities that never
-- held anything and do not qualify are absent -- we do not stamp None on the
-- whole universe, only on genuine drop-offs.
desired_status AS (
  SELECT
    qe.entity_id,
    qe.target_status as desired
  FROM qualifying_entities qe

  UNION ALL

  SELECT
    cs.entity_id,
    '1MC Prospect: None' as desired
  FROM current_status cs
  LEFT JOIN qualifying_entities qe
    ON qe.entity_id = cs.entity_id
  WHERE cs.held_status IN ('1MC Host Prospect', '1MC Leader Prospect')
    AND qe.entity_id IS NULL
),

-- Collapse to one desired status per entity. A drop-off row and a qualifying row
-- cannot coexist for the same entity (the drop-off branch anti-joins qualifiers),
-- so this only guards against duplicate qualifying rows.
desired_deduped AS (
  SELECT entity_id, MIN(desired) as desired
  FROM desired_status
  GROUP BY entity_id
)

SELECT
  eic.campaign_id,
  dd.entity_id,
  dd.desired as field_name,
  -- Internal routing token consumed by updates_needed's
  -- million_conversations_prospect_tag CASE -- not sent to the API.
  'Million Conversations Prospect' as field_group,
  CONCAT(
    '1 Million Conversations:|:1MC Prospect Status:|:',
    dd.desired,
    ':|:standard_response:',
    dd.desired
  ) as sync_string,
  '' as current_value,
  dd.desired as correct_value,
  -- Universal single-select: replace-on-write, and universal taggings cannot be
  -- deleted through the API. Never emit a removal here.
  CAST(NULL AS STRING) as removal_ids
FROM desired_deduped dd
INNER JOIN entities_in_campaigns eic
  ON dd.entity_id = eic.entity_id

-- Network-level anti-join (no campaign_id term) -- see note 3. An entity that
-- already holds the desired status anywhere needs no write.
LEFT JOIN current_status cs
  ON cs.entity_id = dd.entity_id
  AND cs.held_status = dd.desired

-- Entities removed from a campaign still appear in campaigns_entities forever
-- (hard deletes never replicate), so without this the feed emits rows that write
-- as 404 "Entity is not accessible".
LEFT JOIN {{ ref('removed_campaign_entities') }} rem
  ON rem.entity_interact_id = eic.entity_interact_id
 AND rem.campaign_interact_id = eic.campaign_interact_id

WHERE cs.entity_id IS NULL
  AND rem.entity_interact_id IS NULL
