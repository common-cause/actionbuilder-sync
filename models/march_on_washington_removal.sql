-- march_on_washington_removal.sql
-- End-of-life feed for the March on Washington recruitment list: the (campaign, entity)
-- pairs whose `Engagement > Recruitment List > March on Washington` tagging should be
-- deleted. The march is 2026-08-28; run this on/after 2026-08-29.
--
-- Consumed by `sync.py remove_list_taggings --source march_on_washington_removal`.
--
-- This feed emits pairs to CHECK, not tagging ids to delete. The op resolves the live
-- tagging id from the AB API per entity, because our own tagging ids are not reliably
-- available from BQ: sync.py logs add_tagging with a NULL tagging_interact_id, and
-- taggable_logbook replication lags (tag 131 still had not landed in BQ hours after
-- creation). Resolving live also makes the op idempotent and 404-safe.
--
-- Removes the TAG only. Campaign-26 membership and the organizer connection to Carlos
-- are deliberately left in place — after the march these people are his contacts, and
-- dropping them would also lose the connection history. Revisit deliberately if not wanted.
--
-- Two candidate sources, unioned:
--   (a) the frozen list, scoped to campaign 26 — every tagging we wrote went through 26
--   (b) taggable_logbook, any campaign — catches taggings added by hand in the UI
--       (the value is also activated in MD/VA/DC), and is the authority once replicated
-- (b) will be empty until the tag replicates to BQ; (a) is what makes the feed correct
-- before then. Neither is trusted for tagging ids — only for who to check.
--
-- Grain: one row per (campaign, entity) to check.

{% set campaign_26 = '1e7e58fd-efb4-4810-91dc-2e7aac08625a' %}
{% set tag_name = 'March on Washington' %}

-- Numeric id and interact_id both read from LIVE AB (GraphQL createTag response and an
-- API tagging read-back respectively) — not from BQ, which had not replicated the tag.
-- Renames preserve interact_id, so these stay valid if the value is ever relabelled.
{% set tag_id = 131 %}
{% set tag_interact_id = 'bac496d2-e074-4757-8b8a-8b03c63511a3' %}

WITH from_frozen_list AS (
  SELECT
    '{{ campaign_26 }}'  AS campaign_interact_id,
    entity_interact_id
  FROM `proj-tmc-mem-com`.actionbuilder_sync.march_on_washington_list
),

from_logbook AS (
  SELECT DISTINCT
    c.interact_id AS campaign_interact_id,
    e.interact_id AS entity_interact_id
  FROM actionbuilder_cleaned.cln_actionbuilder__taggable_logbook tl
  JOIN actionbuilder_cleaned.cln_actionbuilder__entities e
    ON e.id = tl.taggable_id
  JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns c
    ON c.id = tl.campaign_id
  WHERE tl.tag_id = {{ tag_id }}
    AND tl.taggable_type = 'Entity'
    AND tl.deleted_at IS NULL
),

candidates AS (
  SELECT * FROM from_frozen_list
  UNION DISTINCT
  SELECT * FROM from_logbook
),

last_add AS (
  SELECT entity_interact_id, campaign_interact_id, MAX(executed_at) AS acted_at
  FROM `proj-tmc-mem-com`.actionbuilder_sync.sync_log
  WHERE operation = 'add_tagging'
    AND status = 'ok'
    AND tag_name = '{{ tag_name }}'
    AND entity_interact_id IS NOT NULL
  GROUP BY 1, 2
),

last_delete AS (
  SELECT entity_interact_id, campaign_interact_id, MAX(executed_at) AS acted_at
  FROM `proj-tmc-mem-com`.actionbuilder_sync.sync_log
  WHERE operation = 'delete_tagging'
    AND status IN ('ok', '404')
    AND tag_name = '{{ tag_name }}'
    AND entity_interact_id IS NOT NULL
  GROUP BY 1, 2
),

-- Keeps the feed small on a re-run. Compares the LATEST delete against the LATEST add
-- rather than treating any past delete as final: an entity that was cleared and then
-- re-tagged (a delete/re-add canary, or a re-run of the connect feed) must come back
-- into the feed, or it would keep the tag forever. Not load-bearing either way — the
-- op re-checks live AB state, so a pair that slips through simply finds nothing to
-- delete — but a permanently-excluded pair would never be checked at all.
already_removed AS (
  SELECT d.entity_interact_id, d.campaign_interact_id
  FROM last_delete d
  LEFT JOIN last_add a
    ON  a.entity_interact_id   = d.entity_interact_id
    AND a.campaign_interact_id = d.campaign_interact_id
  WHERE a.acted_at IS NULL OR d.acted_at > a.acted_at
)

SELECT
  c.campaign_interact_id,
  c.entity_interact_id,
  '{{ tag_name }}'         AS tag_name,
  '{{ tag_interact_id }}'  AS tag_interact_id
FROM candidates c
LEFT JOIN already_removed ar
  ON  ar.entity_interact_id   = c.entity_interact_id
  AND ar.campaign_interact_id = c.campaign_interact_id
WHERE ar.entity_interact_id IS NULL
ORDER BY c.campaign_interact_id, c.entity_interact_id
