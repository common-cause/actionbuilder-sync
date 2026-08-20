-- march_on_washington_connects.sql
-- Feed for `sync.py connect_entities --source march_on_washington_connects`.
--
-- Connects each frozen March on Washington target to the Organizing Team campaign
-- (26) and stamps the recruitment-list marker tag in the same API call
-- (update_entity_with_tags POSTs the entity's identifiers to the campaign, which
-- both connects it and adds the tags).
--
-- Reads the FROZEN list table, not march_on_washington_targets — the ranking view
-- re-ranks on every query, so reading it live would change who gets written between
-- runs. See docs/march_on_washington_list.md.
--
-- Deliberately does NOT filter out entities already in campaign 26 (unlike
-- organizing_team_connects, whose job is to keep a nightly feed small). This is a
-- one-shot list: the ~14 targets already in 26 still need the marker tag, and a
-- connect for an already-connected entity is a harmless no-op.
--
-- The marker field is NOT read by current_tag_values / updates_needed, so the
-- nightly pipeline will never remove or overwrite it. Clearing the list after the
-- march is a deliberate manual act.
--
-- Grain: one row per entity (single marker tag each).

{% set campaign_26 = '1e7e58fd-efb4-4810-91dc-2e7aac08625a' %}

WITH already_written AS (
  -- Idempotency: an entity whose marker tag we already logged is not re-sent.
  -- Guards against a re-run duplicating writes if the op is invoked twice.
  SELECT DISTINCT entity_interact_id
  FROM `proj-tmc-mem-com`.actionbuilder_sync.sync_log
  WHERE operation = 'add_tagging'
    AND status IN ('ok', '404')
    AND campaign_interact_id = '{{ campaign_26 }}'
    AND tag_name = 'March on Washington'
    AND entity_interact_id IS NOT NULL
)

SELECT
  '{{ campaign_26 }}'                AS campaign_interact_id,
  m.entity_interact_id,
  'March on Washington'              AS field_name,
  'Engagement:|:Recruitment List:|:March on Washington:|:standard_response:March on Washington'
                                     AS sync_string,
  m.state,
  m.target_rank
FROM `proj-tmc-mem-com`.actionbuilder_sync.march_on_washington_list m
LEFT JOIN already_written aw ON aw.entity_interact_id = m.entity_interact_id
LEFT JOIN {{ ref('suppressed_entities') }} sup
  ON sup.entity_interact_id = m.entity_interact_id
WHERE aw.entity_interact_id IS NULL
  AND sup.entity_interact_id IS NULL
ORDER BY m.target_rank
