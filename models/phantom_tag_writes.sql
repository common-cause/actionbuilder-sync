-- phantom_tag_writes: silent-drop detector for tag writes
--
-- The AB Person Signup Helper silently ignores tag writes to fields that are
-- not activated in the target campaign (returns 200, drops the tagging). This
-- burned VA/DC for three months in 2026: ~24K add_tagging rows logged "ok" in
-- sync_log with nothing in AB, and the sync_log overlay then masked the gap
-- (see docs/tag_taxonomy_redesign_proposal.md §2.1).
--
-- This model flags sync_log `add_tagging ok` rows that have NO evidence in
-- taggable_logbook, grouped by campaign × tag. Any row here means either
-- (a) a field is missing/deactivated in that campaign — the VA/DC failure
-- class — or (b) taggable_logbook replication has stalled longer than the
-- grace period (compare with the replication timestamps before panicking).
--
-- Matching is deliberately loose to avoid false positives:
--   * evidence = ANY logbook row (live or deleted) for the entity + tag —
--     re-writes of an existing value may not create a new logbook row, so no
--     time-window matching on the specific write.
--   * universal-section taggings (OFP etc.) log campaign_id = NULL in
--     taggable_logbook, so a NULL-campaign row is accepted as evidence.
--   * tag matched by interact_id when sync_log has it, by name otherwise --
--     EXCEPT for pre-cutover writes to the four Block G duplicated names, whose
--     logged interact_id is unreliable; those match by name (see below).
--   * 7-day grace period for logbook replication lag.
--
-- Expected state: zero rows. Eyeball after adding a campaign or a new tag
-- field; candidate for the Slack replication sentinel when that ships.

WITH ok_adds AS (
  SELECT
    sl.entity_interact_id,
    sl.campaign_interact_id,
    sl.tag_name,
    sl.tag_interact_id,
    sl.executed_at,
    -- Taxonomy Block G collision (measured 2026-08-26). Before the cutover deploy
    -- pinned BLOCK_G_TAG_IDS, `_get_tag_map` resolved these four DUPLICATED value
    -- names off an unordered query, so a pre-cutover row can carry the NEW tag
    -- uuid while the sync string still pointed at the LEGACY field -- and the
    -- tagging physically landed in the legacy field. Matching such a row on
    -- interact_id can never succeed, so a write that DID land is reported as a
    -- phantom forever: that is the whole of the 24-write / 6-row floor dated
    -- 2026-08-18 (all 24 verified against a same-name logbook row within 30h of
    -- the write; legacy tags 64/74 archived by Block H wave 3 on 2026-08-24).
    --
    -- Fix, deliberately narrow: for these rows trust the NAME, not the logged id.
    -- Name-matching is sound here because both twins share the name, so a landing
    -- in either field counts as evidence -- and it keeps the detector able to see
    -- a genuine pre-cutover drop, which simply excluding the rows would not.
    -- Same discriminator (name + cutover TIME, never the logged id) and same
    -- cutover constant as current_tag_values.sql.
    sl.tag_name IN (
      'First Event Attended',
      'Most Recent Event Attended',
      'Top State Action Taker',
      'Top National Action Network Activist'
    ) AND sl.executed_at < TIMESTAMP('2026-08-19 19:00:00+00') AS logged_id_untrustworthy
  FROM `proj-tmc-mem-com`.actionbuilder_sync.sync_log sl
  WHERE sl.operation = 'add_tagging'
    AND sl.status = 'ok'
    AND sl.tag_name IS NOT NULL
    AND sl.executed_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
),

adds AS (
  SELECT
    o.*,
    e.id AS entity_id,
    c.id AS campaign_id,
    c.name AS campaign_name
  FROM ok_adds o
  JOIN actionbuilder_cleaned.cln_actionbuilder__entities e
    ON e.interact_id = o.entity_interact_id
  LEFT JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns c
    ON c.interact_id = o.campaign_interact_id
),

-- All logbook rows, INCLUDING deleted ones: a deleted tagging still proves
-- the write landed. (current_tag_values wants live state; we want evidence.)
logbook AS (
  SELECT
    tl.taggable_id AS entity_id,
    tl.campaign_id,
    t.interact_id AS tag_interact_id,
    t.name AS tag_name
  FROM actionbuilder_cleaned.cln_actionbuilder__taggable_logbook tl
  JOIN actionbuilder_cleaned.cln_actionbuilder__tags t
    ON t.id = tl.tag_id
  WHERE tl.taggable_type = 'Entity'
),

unmatched AS (
  SELECT a.*
  FROM adds a
  LEFT JOIN logbook lb
    ON lb.entity_id = a.entity_id
    AND (
      (a.tag_interact_id IS NOT NULL AND NOT a.logged_id_untrustworthy
        AND lb.tag_interact_id = a.tag_interact_id)
      OR ((a.tag_interact_id IS NULL OR a.logged_id_untrustworthy)
        AND lb.tag_name = a.tag_name)
    )
    AND (lb.campaign_id = a.campaign_id OR lb.campaign_id IS NULL)
  WHERE lb.entity_id IS NULL
)

SELECT
  campaign_id,
  campaign_name,
  tag_name,
  COUNT(*) AS n_unmatched_writes,
  COUNT(DISTINCT entity_interact_id) AS n_entities,
  MIN(DATE(executed_at)) AS first_write,
  MAX(DATE(executed_at)) AS last_write
FROM unmatched
GROUP BY 1, 2, 3
ORDER BY n_unmatched_writes DESC
