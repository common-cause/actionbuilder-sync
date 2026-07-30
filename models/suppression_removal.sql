-- suppression_removal.sql
-- Removal feed for sync.py remove_suppressed: every active-campaign membership of a
-- suppressed entity (see suppressed_entities / suppression_list), minus memberships
-- already removed per sync_log. Self-clears after the op runs ('remove_suppressed'
-- is listed in removed_campaign_entities).
--
-- Suppression removes the entity from ALL active campaigns (including Organizing
-- Team and Test) — the entity itself continues to exist network-level in AB, but
-- the insert/connect feed guards prevent it from ever being re-added by the sync.
--
-- Grain: one row per (suppressed entity, active campaign membership).

WITH memberships AS (
  SELECT
    se.entity_interact_id AS delete_interact_id,
    e.first_name          AS delete_first_name,
    e.last_name           AS delete_last_name,
    c.interact_id         AS campaign_interact_id,
    c.name                AS campaign_name,
    se.reason             AS removal_reason
  FROM {{ ref('suppressed_entities') }} se
  JOIN actionbuilder_cleaned.cln_actionbuilder__entities e ON e.id = se.entity_id
  JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns_entities ce ON ce.entity_id = se.entity_id
  JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns c
    ON c.id = ce.campaign_id AND c.status = 'active'
)

SELECT DISTINCT m.*
FROM memberships m
LEFT JOIN {{ ref('removed_campaign_entities') }} rce
  ON rce.entity_interact_id  = m.delete_interact_id
 AND rce.campaign_interact_id = m.campaign_interact_id
WHERE rce.entity_interact_id IS NULL
ORDER BY delete_last_name, campaign_name
