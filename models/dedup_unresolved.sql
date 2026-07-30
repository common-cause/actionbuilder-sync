-- dedup_unresolved: ambiguous duplicate pairs that have not yet been reviewed.
--
-- This is the working review queue. It is dedup_ambiguous minus any pairs
-- that already have a decision in actionbuilder_sync.dedup_resolutions.
--
-- Two uses:
--
--   1. Human / AI review queue: read this view to see what needs a decision.
--      Use scripts/add_resolution.py to record decisions. On the next dbt run,
--      resolved pairs disappear from here and (if MERGE) appear in dedup_candidates.
--
--   2. Hold-out signal for deduplicated_names_to_load: new records whose
--      voterbase_id is involved in an unresolved pair are withheld from
--      insertion until the ambiguity is resolved. This prevents creating a
--      third AB entity for a person whose two existing entities we haven't
--      figured out yet.
--
-- NOTE: actionbuilder_sync.dedup_resolutions is a BQ table managed outside dbt.
-- Create it once using scripts/create_dedup_resolutions.sql before running dbt.
-- It starts empty; all rows in dedup_ambiguous appear here on first run.

-- DEFER rows do NOT hide a pair: per add_resolution.py's documented semantics a
-- deferred pair stays in the queue (fixed 2026-07-30 — the join previously matched
-- any decision row, so a DEFER silently buried the pair).
SELECT da.*
FROM {{ ref('dedup_ambiguous') }} da
LEFT JOIN `proj-tmc-mem-com`.actionbuilder_sync.dedup_resolutions dr
  ON da.pair_id = dr.pair_id
 AND dr.decision != 'DEFER'
WHERE dr.pair_id IS NULL

ORDER BY da.signal_type, da.entity_a_last_name, da.entity_a_first_name
