-- ep_shift_tags.sql
-- Stamps Election Protection shift-year flags on ActionBuilder entities:
-- `Election Protection > Election Protection Shifts > EP Shift Worked <year>`.
-- Output is shaped for direct UNION into updates_to_apply inside updates_needed.sql.
--
-- New in taxonomy Block G (2026-08-19). Until now the EP shift history was a load
-- QUALIFIER only -- it decided who got created in AB, but the fact of having
-- worked a shift was never written onto the person. Block F renamed the field to
-- `Election Protection Shifts` and added the 2022/2026 responses alongside the
-- existing 2024 one, so all three years can now be stamped.
--
-- The field is UNIVERSAL and MULTI-select (verified against live AB 2026-08-19:
-- field_is_universal=true, allow_multiple_responses=true). Three consequences:
--
--   1. Additive only. We ADD missing year flags and never remove any -- a shift
--      worked in 2022 stays true forever. Universal taggings are also
--      API-undeletable, so a removal would 404 regardless.
--   2. Idempotency keys on tag INTERACT_IDS, not names (the Block D lesson: a
--      model that FILTERS on interact_id but JOINS on tag_name silently unmatches
--      every row the next time the value is renamed). The interact_id is
--      therefore also the source of the name we compare on.
--   3. GRAIN IS (entity, tag), NOT (entity, campaign, tag). One network-level tag
--      object is shared by every campaign and a tagging written under campaign X
--      reads back through campaign Y, so a per-campaign "already has it" check
--      invents work that can never drain.
--
-- Interact_ids are hardcoded because BQ has not replicated the Block F objects:
-- as of 2026-08-19 tag 45 still reads its pre-rename name '2024' in
-- cln_actionbuilder__tags and tags 129/130 are absent entirely. Read from live AB
-- via list_tags. Same rationale as OFP_UNIVERSAL_TAG_IDS in sync.py.

{% set ep_shift_years = [
    ('2022', 'b9612ad8-840c-4e10-953d-7b0e5590ee5f'),
    ('2024', 'f53c7eae-b20e-412b-a340-825cb5be5dc9'),
    ('2026', 'ea8cd848-3b90-4afe-b7d5-06d6fde32f92'),
] %}

WITH shift_workers AS (
  -- master_load_qualifiers collapses one row per person with a comma-joined
  -- `qualification_reasons` string ('EP Shift 2022, EP Shift 2026, ...'), so a
  -- person who worked shifts in several years yields one row per year here.
  SELECT DISTINCT
    LOWER(TRIM(mlq.email)) as email_normalized,
    y.shift_year,
    CONCAT('EP Shift Worked ', y.shift_year) as ep_tag
  FROM {{ ref('master_load_qualifiers') }} mlq
  CROSS JOIN UNNEST([
    {%- for year, _ in ep_shift_years %}
    STRUCT('{{ year }}' AS shift_year){{ ',' if not loop.last }}
    {%- endfor %}
  ]) y
  WHERE mlq.email IS NOT NULL
    AND mlq.qualification_reasons LIKE CONCAT('%EP Shift ', y.shift_year, '%')
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

ep_entity_tags AS (
  -- Map EP shift workers to AB entities via email
  SELECT DISTINCT
    ee.entity_id,
    sw.ep_tag
  FROM shift_workers sw
  INNER JOIN entity_emails ee
    ON sw.email_normalized = ee.email_normalized
),

entities_in_campaigns AS (
  -- interact_ids are carried through so the removed-entity anti-join below can
  -- match removed_campaign_entities, which is keyed on interact_ids.
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

current_ep_tags AS (
  -- Which EP shift-year flags does each entity already hold? Keyed on the
  -- universal tag interact_ids, network-level grain (see notes 2 and 3).
  SELECT DISTINCT
    entity_id,
    CASE tag_interact_id
      {%- for year, iid in ep_shift_years %}
      WHEN '{{ iid }}' THEN 'EP Shift Worked {{ year }}'
      {%- endfor %}
    END as ep_tag
  FROM {{ ref('current_tag_values') }}
  WHERE tag_interact_id IN (
    {%- for year, iid in ep_shift_years %}
    '{{ iid }}'{{ ',' if not loop.last }}  -- EP Shift Worked {{ year }}
    {%- endfor %}
  )
)

-- Entity+campaign+tag combos that need to be ADDED (not already present)
SELECT
  eic.campaign_id,
  eet.entity_id,
  eet.ep_tag as field_name,
  -- Internal routing token consumed by updates_needed's ep_shift_tag CASE --
  -- not sent to the API.
  'Election Protection Shifts' as field_group,
  CONCAT(
    'Election Protection:|:Election Protection Shifts:|:',
    eet.ep_tag,
    ':|:standard_response:',
    eet.ep_tag
  ) as sync_string,
  '' as current_value,
  eet.ep_tag as correct_value,
  -- Universal + additive: never emit a removal.
  CAST(NULL AS STRING) as removal_ids
FROM ep_entity_tags eet
INNER JOIN entities_in_campaigns eic
  ON eet.entity_id = eic.entity_id

-- Network-level anti-join (no campaign_id term) -- see note 3.
LEFT JOIN current_ep_tags cet
  ON eet.entity_id = cet.entity_id
  AND eet.ep_tag = cet.ep_tag

-- Entities removed from a campaign still appear in campaigns_entities forever
-- (hard deletes never replicate), so without this the feed emits rows that write
-- as 404 "Entity is not accessible".
LEFT JOIN {{ ref('removed_campaign_entities') }} rem
  ON rem.entity_interact_id = eic.entity_interact_id
 AND rem.campaign_interact_id = eic.campaign_interact_id

WHERE cet.entity_id IS NULL
  AND rem.entity_interact_id IS NULL
