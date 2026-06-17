-- organizing_team_connects.sql
-- OFP attendees who already exist as ActionBuilder entities and should be CONNECTED
-- to the Organizing Team campaign (id 26) with their universal OFP competencies stamped.
--
-- The "Organizing For Power" field is universal, so stamping competencies through any
-- campaign sets them network-wide; connecting the entity to campaign 26 makes them
-- visible there. The connect itself is performed by sync.py connect_entities via
-- ab.update_entity_with_tags (POST person.identifiers -> connects + adds tags).
--
-- Candidate pool = entities in STATE campaigns only (Test and campaign 26 excluded):
--   - Test holds a legacy duplicate load; its entities must not be connected.
--   - Already-in-26 entities are filtered out (BQ campaigns_entities + sync_log overlay).
-- Pick-one rule for people with multiple state-campaign entities: most-recently-updated
-- entity (deterministic; entity_id breaks ties). Home-state preference is intentionally
-- not attempted here — attendee state is not reliably known from Mobilize.
--
-- Grain: one row per (entity_interact_id, competency) with a ready universal-field sync string.

{% set campaign_26 = '1e7e58fd-efb4-4810-91dc-2e7aac08625a' %}

WITH state_campaigns AS (
  -- Active campaigns that are real state campaigns: exclude Test and Organizing Team (26)
  SELECT id AS campaign_id, interact_id, name
  FROM actionbuilder_cleaned.cln_actionbuilder__campaigns
  WHERE status = 'active'
    AND name != 'Test'
    AND id != 26
),

ofp_people AS (
  SELECT DISTINCT email_normalized
  FROM {{ ref('ofp_universe') }}
),

entity_emails AS (
  SELECT
    owner_id AS entity_id,
    LOWER(TRIM(email)) AS email_normalized
  FROM actionbuilder_cleaned.cln_actionbuilder__emails
  WHERE owner_type = 'Entity'
    AND status IN ('verified', 'user_added')
    AND email IS NOT NULL
),

-- OFP attendees matched to state-campaign entities
matched AS (
  SELECT
    op.email_normalized,
    e.id                AS entity_id,
    e.interact_id       AS entity_interact_id,
    e.updated_at        AS entity_updated_at,
    sc.name             AS campaign_name
  FROM ofp_people op
  JOIN entity_emails ee ON ee.email_normalized = op.email_normalized
  JOIN actionbuilder_cleaned.cln_actionbuilder__entities e ON e.id = ee.entity_id
  JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns_entities ce ON ce.entity_id = e.id
  JOIN state_campaigns sc ON sc.campaign_id = ce.campaign_id
),

-- Pick one entity per person (most recently updated)
pick_one AS (
  SELECT *
  FROM matched
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY email_normalized
    ORDER BY entity_updated_at DESC, entity_id DESC
  ) = 1
),

-- Entities already in campaign 26 per the BQ snapshot
already_in_26_bq AS (
  SELECT DISTINCT e.interact_id AS entity_interact_id
  FROM actionbuilder_cleaned.cln_actionbuilder__campaigns_entities ce
  JOIN actionbuilder_cleaned.cln_actionbuilder__entities e ON e.id = ce.entity_id
  WHERE ce.campaign_id = 26
),

-- Entities already connected to 26 per our sync_log (covers replication lag)
already_in_26_log AS (
  SELECT DISTINCT entity_interact_id
  FROM `proj-tmc-mem-com`.actionbuilder_sync.sync_log
  WHERE operation = 'connect_entity'
    AND status IN ('ok', '404')
    AND campaign_interact_id = '{{ campaign_26 }}'
    AND entity_interact_id IS NOT NULL
),

connectable AS (
  SELECT p.email_normalized, p.entity_interact_id
  FROM pick_one p
  LEFT JOIN already_in_26_bq  bq  ON bq.entity_interact_id  = p.entity_interact_id
  LEFT JOIN already_in_26_log lg  ON lg.entity_interact_id  = p.entity_interact_id
  WHERE bq.entity_interact_id IS NULL
    AND lg.entity_interact_id IS NULL
)

SELECT
  '{{ campaign_26 }}'      AS campaign_interact_id,
  c.entity_interact_id,
  u.competency             AS field_name,
  u.sync_string
FROM connectable c
JOIN {{ ref('ofp_universe') }} u ON u.email_normalized = c.email_normalized
ORDER BY c.entity_interact_id, u.competency
