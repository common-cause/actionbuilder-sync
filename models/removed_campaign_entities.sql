-- removed_campaign_entities: canonical set of (entity, campaign) pairs that sync.py
-- has removed from a campaign, per sync_log.
--
-- Why this exists (the "removal gap"): TMC's incremental replication never captures
-- hard deletes — removing an entity from a campaign produces no updated_at change, so
-- BQ's cln_actionbuilder__campaigns_entities shows removed entities indefinitely
-- (documented replication gap #1). Any model that reasons about *current* campaign
-- membership from the BQ snapshot must subtract this set, or it will re-surface
-- entities that are already gone from live ActionBuilder. (This is the same class of
-- problem current_tag_values solves for taggable_logbook.)
--
-- All entity-from-campaign removal operations are unioned here so that downstream
-- feeds get coverage from one place — when a new removal op is added to sync.py,
-- add its operation string to the list below and every consumer is covered.
--   remove_from_campaign   — remove_records (dedup duplicate removal)
--   remove_ep_external     — remove_ep_externals (partner-org EP-shift cleanup)
--   remove_mobilize_external — remove_mobilize_externals (Rule A/B anti-poaching cleanup)
-- status IN ('ok','404') both mean the entity is absent from the campaign (404 = the
-- delete found it already gone, i.e. desired state achieved).
--
-- Semantics are "ever removed": a re-add to the same campaign is not modelled (consistent
-- with dedup_candidates). The removal feeds never re-add removed entities, so this is safe
-- and conservative there.

SELECT DISTINCT
  entity_interact_id,
  campaign_interact_id
FROM `proj-tmc-mem-com`.actionbuilder_sync.sync_log
WHERE operation IN ('remove_from_campaign', 'remove_ep_external', 'remove_mobilize_external')
  AND status IN ('ok', '404')
  AND entity_interact_id IS NOT NULL
  AND campaign_interact_id IS NOT NULL
