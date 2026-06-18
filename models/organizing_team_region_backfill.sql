-- organizing_team_region_backfill: campaign-26 entities missing an AB address state,
-- with the authoritative (zip-derived) state to stamp so per-state AB queries are reliable.
--
-- Why: OT members filter the Organizing Team wallchart by state, which queries the entity's
-- address state (cln_actionbuilder__addresses.state — where our `region` write lands). Inserts
-- stamp region=state, but connected entities only carry whatever address their state-campaign
-- load set, so older ones can be missing it. This feed fills those gaps from ofp_universe's
-- authoritative zip->state (the same source that routed them), via the backfill_region sync op
-- (update_person -> postal_addresses.region).
--
-- SAFE BY DESIGN: emits only entities whose AB address state is MISSING. Entities whose AB state
-- merely differs from the zip-derived state are surfaced (mismatch flag) but NOT auto-overwritten
-- — a different AB state may be a deliberate/ newer address. Review those manually.
--
-- NOTE: depends on campaign-26 membership in cln_actionbuilder__campaigns_entities, which lags
-- after a connect/insert run (replication gap). Expect ~0 rows until the latest connects replicate.

{% set campaign_26 = '1e7e58fd-efb4-4810-91dc-2e7aac08625a' %}

WITH members AS (
  SELECT DISTINCT entity_id
  FROM actionbuilder_cleaned.cln_actionbuilder__campaigns_entities
  WHERE campaign_id = 26
),
removed AS (
  SELECT entity_interact_id
  FROM {{ ref('removed_campaign_entities') }}
  WHERE campaign_interact_id = '{{ campaign_26 }}'
),
ent AS (
  SELECT e.id AS entity_id, e.interact_id AS entity_interact_id, e.first_name, e.last_name
  FROM members m
  JOIN actionbuilder_cleaned.cln_actionbuilder__entities e ON e.id = m.entity_id
  WHERE e.interact_id NOT IN (SELECT entity_interact_id FROM removed)
),
-- Current AB address state (most recent non-empty), per entity
cur_state AS (
  SELECT owner_id AS entity_id,
         ARRAY_AGG(NULLIF(UPPER(TRIM(state)), '') IGNORE NULLS ORDER BY updated_at DESC)[SAFE_OFFSET(0)] AS ab_state
  FROM actionbuilder_cleaned.cln_actionbuilder__addresses
  WHERE owner_type = 'Entity'
  GROUP BY owner_id
),
-- Authoritative zip-derived state via the entity's emails -> ofp_universe
ent_email AS (
  SELECT m.entity_id, LOWER(TRIM(e.email)) AS email
  FROM members m
  JOIN actionbuilder_cleaned.cln_actionbuilder__emails e
    ON e.owner_id = m.entity_id AND e.owner_type = 'Entity' AND e.email IS NOT NULL
),
ofp AS (
  SELECT email_normalized, ANY_VALUE(state) AS state, ANY_VALUE(zip_code) AS zip_code
  FROM {{ ref('ofp_universe') }}
  GROUP BY email_normalized
),
authoritative AS (
  SELECT ee.entity_id,
         ARRAY_AGG(UPPER(o.state) IGNORE NULLS ORDER BY o.state)[SAFE_OFFSET(0)] AS zip_state,
         ARRAY_AGG(o.zip_code   IGNORE NULLS)[SAFE_OFFSET(0)]                    AS zip_code
  FROM ent_email ee
  JOIN ofp o ON o.email_normalized = ee.email
  GROUP BY ee.entity_id
),
-- A primary-ish email for the update_person payload
prim_email AS (
  SELECT owner_id AS entity_id,
         ARRAY_AGG(email ORDER BY (status = 'verified') DESC, created_at)[SAFE_OFFSET(0)] AS email
  FROM actionbuilder_cleaned.cln_actionbuilder__emails
  WHERE owner_type = 'Entity' AND email IS NOT NULL
  GROUP BY owner_id
)
SELECT
  '{{ campaign_26 }}'    AS campaign_interact_id,
  ent.entity_interact_id,
  ent.first_name,
  ent.last_name,
  pe.email,
  a.zip_state            AS state,
  a.zip_code,
  cs.ab_state            AS current_ab_state   -- NULL = missing (this feed); non-NULL+differs = review only
FROM ent
JOIN authoritative a USING (entity_id)
LEFT JOIN cur_state cs USING (entity_id)
LEFT JOIN prim_email pe USING (entity_id)
WHERE a.zip_state IS NOT NULL
  AND cs.ab_state IS NULL          -- missing only; never overwrite an existing AB state
