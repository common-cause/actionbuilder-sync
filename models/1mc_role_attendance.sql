-- 1mc_role_attendance.sql
-- Identifies ActionBuilder entities who completed 1MC trainings (Leader or Host)
-- and are missing the corresponding Campaign Role tag.  Output is shaped for
-- direct UNION into updates_to_apply inside updates_needed.sql.
--
-- Multiselect / additive-only: we ADD missing tags, never remove existing ones.
-- Same pattern as ofp_attendance.sql.
--
-- NOTE (2026-08-25): this model returns 0 rows because seeds/1mc_training_map.csv is
-- header-only -- no 1MC training timeslot has ever been mapped, so the Host/Leader
-- half of the 1MC pipeline has been dormant since it shipped. That is a seed gap,
-- not a defect here. The correctness work below matters for when the seed is filled.
--
-- Hardened 2026-08-25 to the pattern 1mc_prospects.sql already proves; identical
-- reasoning to 1mc_participants.sql, whose header carries the full write-up:
--
--   1. GRAIN IS (entity, tag), NOT (entity, campaign, tag). '1MC Host' and
--      '1MC Leader' (cat 24) are UNIVERSAL tags -- sync.py
--      UNIVERSAL_TAG_IDS_NO_DELETE, "the whole 1 Million Conversations section is
--      universal", verified against live AB 2026-08-19. A universal tagging reads
--      back with campaign_id NULL, so a per-campaign anti-join never matches it and
--      re-emits every campaign forever (1mc_prospects note 3 -- the 219 undrainable
--      OFP rows).
--   2. The anti-join keys on tag INTERACT_ID, not tag_name. Both of these responses
--      were renamed ('Host' -> '1MC Host' in Block A, Block E for the rest), and the
--      Block D lesson is that ofp_attendance filtered on interact_id but JOINED on
--      tag_name, silently unmatching 1,452 of 1,452 rows.
--   3. campaigns_entities carries duplicate (entity, campaign) rows (795 pairs on
--      2026-08-25) so the membership CTE is DISTINCT, and entities removed from a
--      campaign linger there forever (replication gap #1) so
--      removed_campaign_entities is subtracted.

{% set tag_1mc_host   = '8d8cd0ec-2f45-4338-85c9-53179c1f63a8' %}
{% set tag_1mc_leader = '09a3ff4c-a872-4cdc-a827-f4d229cf4eed' %}

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
    tp.role_tag,
    -- Resolve the seed's role NAME to the stable tag interact_id, so the anti-join
    -- below survives a rename (header note 2). role_tag is still carried through
    -- because the sync string is built from it. A seed row naming a role we have no
    -- id for resolves to NULL and is dropped in the final WHERE -- better to emit
    -- nothing than to emit a row that can never match and re-fires nightly.
    CASE tp.role_tag
      WHEN '1MC Host'   THEN '{{ tag_1mc_host }}'
      WHEN '1MC Leader' THEN '{{ tag_1mc_leader }}'
    END as role_tag_interact_id
  FROM training_participations tp
  INNER JOIN entity_emails ee
    ON tp.email_normalized = ee.email_normalized
),

entities_in_campaigns AS (
  -- All entities in active campaigns. DISTINCT, and interact_ids carried through
  -- for the removed-entity anti-join. See header note 3.
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

current_role_tags AS (
  -- Which 1MC Campaign Role tags does each entity already hold, anywhere on the
  -- network? Entity+tag grain, no campaign term, keyed on interact_id -- header
  -- notes 1 and 2.
  SELECT DISTINCT
    entity_id,
    tag_interact_id as role_tag_interact_id
  FROM {{ ref('current_tag_values') }}
  WHERE tag_interact_id IN ('{{ tag_1mc_host }}', '{{ tag_1mc_leader }}')
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

-- Network-level anti-join (no campaign_id term) -- see header note 1.
LEFT JOIN current_role_tags crt
  ON ret.entity_id = crt.entity_id
  AND ret.role_tag_interact_id = crt.role_tag_interact_id

-- Entities removed from a campaign still appear in campaigns_entities forever
-- (hard deletes never replicate); without this the write 404s and re-emits nightly.
LEFT JOIN {{ ref('removed_campaign_entities') }} rem
  ON rem.entity_interact_id = eic.entity_interact_id
 AND rem.campaign_interact_id = eic.campaign_interact_id

WHERE crt.entity_id IS NULL
  AND rem.entity_interact_id IS NULL
  -- Drop seed rows naming a role with no known interact_id (see role_entity_tags).
  AND ret.role_tag_interact_id IS NOT NULL
