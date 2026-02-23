-- create_dedup_resolutions.sql
--
-- One-time DDL: creates the dedup_resolutions table in the actionbuilder_sync dataset.
-- This table lives outside dbt and is the authoritative store for human/AI decisions
-- about ambiguous duplicate entity pairs surfaced by dedup_unresolved.
--
-- Run once before the first `dbt run` that uses dedup_unresolved or dedup_candidates
-- (resolved_merge tier). After creation, the table starts empty — all pairs in
-- dedup_unresolved will be visible for review.
--
-- To add resolutions, use scripts/add_resolution.py or INSERT directly.

CREATE TABLE IF NOT EXISTS `proj-tmc-mem-com`.actionbuilder_sync.dedup_resolutions (

  -- Canonical pair identifier: CONCAT(LEAST(iid), ':', GREATEST(iid))
  -- Matches the pair_id in dedup_ambiguous / dedup_unresolved.
  pair_id STRING NOT NULL,

  -- The two entities in the pair (same canonicalization as dedup_ambiguous)
  entity_a_interact_id STRING NOT NULL,
  entity_b_interact_id STRING NOT NULL,

  -- Resolution decision:
  --   MERGE_A_INTO_B  — delete entity_a, keep entity_b
  --   MERGE_B_INTO_A  — delete entity_b, keep entity_a
  --   KEEP_BOTH       — confirmed distinct people; suppress future flagging
  --   DEFER           — needs more information; leave in unresolved queue
  decision STRING NOT NULL,

  -- For MERGE decisions: which interact_id to delete and which to keep.
  -- Must be populated when decision is MERGE_A_INTO_B or MERGE_B_INTO_A.
  -- NULL for KEEP_BOTH and DEFER.
  delete_interact_id STRING,
  keep_interact_id   STRING,

  -- Human-readable rationale for the decision
  reason STRING,

  -- Who made the decision: 'human:rob', 'ai:claude-sonnet-4-6', etc.
  resolved_by STRING,

  -- When the decision was recorded
  resolved_at TIMESTAMP

)
OPTIONS (
  description = 'Resolutions for ambiguous ActionBuilder entity duplicate pairs. '
                'Populated by human review or AI via add_resolution.py. '
                'MERGE decisions flow into dedup_candidates (resolved_merge tier) on next dbt run.'
);
