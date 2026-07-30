-- create_suppression_list.sql
--
-- One-time DDL: creates the suppression_list table in the actionbuilder_sync dataset.
-- Managed OUTSIDE dbt (like dedup_resolutions) so entries survive dbt runs and no
-- person-identifying data ever lands in the repo. Populated via scripts/add_suppression.py.
--
-- Purpose: a curated do-not-sync list. A person on this list is (a) removed from all
-- active campaigns by the remove_suppressed sync op (feed: suppression_removal), and
-- (b) blocked from re-entering by anti-joins in the insert/connect feeds
-- (deduplicated_names_to_load, organizing_team_inserts, organizing_team_connects).
--
-- Each row lists ONE identifier (email, person_id, or entity_interact_id) — add one
-- row per identifier for the same person. Matching is aggressive: an entity is
-- suppressed if its interact_id is listed OR it owns a listed email; an insert-feed
-- row is suppressed if its email or person_id is listed.

CREATE TABLE IF NOT EXISTS `proj-tmc-mem-com`.actionbuilder_sync.suppression_list (
  email STRING,               -- normalized lower/trim (one of the three identifiers)
  person_id STRING,           -- core_enhanced person_id
  entity_interact_id STRING,  -- specific AB entity UUID
  reason STRING NOT NULL,     -- why this person is suppressed
  added_by STRING NOT NULL,   -- 'human:rob', 'ai:claude-fable-5', ...
  added_at TIMESTAMP NOT NULL
);
