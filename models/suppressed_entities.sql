-- suppressed_entities.sql
-- Resolves the curated suppression_list (BQ table managed outside dbt; see
-- scripts/create_suppression_list.sql) to concrete ActionBuilder entities.
--
-- An entity is suppressed if:
--   - its interact_id is listed directly, OR
--   - it owns a listed email (any status — suppression is deliberately aggressive)
--
-- person_id rows in suppression_list are enforced in the insert feeds (which carry
-- person_id on each row), not here — entity-level resolution via person_id would
-- require the identity-hub join and email/interact matching already covers live entities.
--
-- Consumers: suppression_removal (campaign removal feed), organizing_team_connects
-- (never connect a suppressed entity to campaign 26).
--
-- Grain: one row per suppressed entity.

WITH list AS (
  SELECT
    LOWER(TRIM(email))  AS email,
    entity_interact_id,
    reason
  FROM `proj-tmc-mem-com`.actionbuilder_sync.suppression_list
),

by_interact AS (
  SELECT e.id AS entity_id, e.interact_id, l.reason
  FROM actionbuilder_cleaned.cln_actionbuilder__entities e
  JOIN list l ON l.entity_interact_id = e.interact_id
),

by_email AS (
  SELECT e.id AS entity_id, e.interact_id, l.reason
  FROM list l
  JOIN actionbuilder_cleaned.cln_actionbuilder__emails em
    ON LOWER(TRIM(em.email)) = l.email
   AND em.owner_type = 'Entity'
  JOIN actionbuilder_cleaned.cln_actionbuilder__entities e
    ON e.id = em.owner_id
  WHERE l.email IS NOT NULL
)

SELECT
  entity_id,
  interact_id AS entity_interact_id,
  ANY_VALUE(reason) AS reason
FROM (
  SELECT * FROM by_interact
  UNION ALL
  SELECT * FROM by_email
)
GROUP BY entity_id, interact_id
