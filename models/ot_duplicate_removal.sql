-- ot_duplicate_removal.sql
-- One-shot cleanup feed: the NEWER twin of each campaign-26 duplicate entity pair
-- created by the organizing_team_inserts double-insert bug (2026-06-20/21).
--
-- The bug: the insert feed's "already in AB" check used only the BQ mirror, whose
-- ~1-day replication lag meant the 2026-06-21 nightly re-inserted all 70 people the
-- 2026-06-20 run had just created (sync_log shows a 70/70 person_id overlap). The
-- model has since been fixed with a sync_log already_inserted overlay; this feed
-- removes the leftover duplicates. Consumed by sync.py remove_ot_duplicates, which
-- logs operation='remove_ot_duplicate' (listed in removed_campaign_entities, so this
-- feed self-clears and organizing_team_assignments drops removed twins).
--
-- Scope guards — a pair qualifies only if ALL hold (the 16 launch-window pairs from
-- 6/17-18 and the 1 legacy 2025 pair are deliberately excluded; they need per-pair
-- triage):
--   1. exactly 2 campaign-26 member entities share the email
--   2. BOTH entities were created on 2026-06-20 or 2026-06-21 (the bug window)
--   3. the twin to remove is the newer one (later created_at; entity id tiebreak)
--   4. the twin to remove belongs to NO other campaign per the mirror
--
-- Grain: one row per entity to remove (~70 expected on first run, then 0).

{% set campaign_26 = '1e7e58fd-efb4-4810-91dc-2e7aac08625a' %}

WITH members26 AS (
  SELECT e.id, e.interact_id, e.first_name, e.last_name, e.created_at
  FROM actionbuilder_cleaned.cln_actionbuilder__campaigns_entities ce
  JOIN actionbuilder_cleaned.cln_actionbuilder__entities e ON e.id = ce.entity_id
  WHERE ce.campaign_id = 26
),

-- (entity, email) grain — collapses duplicate email rows
entity_emails AS (
  SELECT
    LOWER(TRIM(em.email))    AS email_norm,
    m.id,
    ANY_VALUE(m.interact_id) AS interact_id,
    ANY_VALUE(m.first_name)  AS first_name,
    ANY_VALUE(m.last_name)   AS last_name,
    ANY_VALUE(m.created_at)  AS created_at
  FROM members26 m
  JOIN actionbuilder_cleaned.cln_actionbuilder__emails em
    ON em.owner_id = m.id AND em.owner_type = 'Entity'
  WHERE em.email IS NOT NULL
  GROUP BY 1, 2
),

-- Emails whose 2 member entities were BOTH created in the double-insert window
bug_pair_emails AS (
  SELECT email_norm
  FROM entity_emails
  GROUP BY email_norm
  HAVING COUNT(*) = 2
     AND COUNTIF(DATE(created_at) IN ('2026-06-20', '2026-06-21')) = 2
),

-- Newer twin of each pair
newer_twin AS (
  SELECT ee.*
  FROM entity_emails ee
  JOIN bug_pair_emails USING (email_norm)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ee.email_norm
    ORDER BY ee.created_at DESC, ee.id DESC
  ) = 1
),

-- Safety guard: the twin we remove must live ONLY in campaign 26
campaign_counts AS (
  SELECT entity_id, COUNT(DISTINCT campaign_id) AS n_campaigns
  FROM actionbuilder_cleaned.cln_actionbuilder__campaigns_entities
  GROUP BY entity_id
),

-- Already removed per sync_log (hard deletes never replicate; self-clearing)
already_removed AS (
  SELECT entity_interact_id
  FROM {{ ref('removed_campaign_entities') }}
  WHERE campaign_interact_id = '{{ campaign_26 }}'
)

SELECT DISTINCT
  '{{ campaign_26 }}'  AS campaign_interact_id,
  nt.interact_id       AS delete_interact_id,
  nt.first_name        AS delete_first_name,
  nt.last_name         AS delete_last_name,
  nt.email_norm,
  DATE(nt.created_at)  AS created_date,
  'organizing_team_inserts double-insert 2026-06-20/21 (newer twin)' AS removal_reason
FROM newer_twin nt
JOIN campaign_counts cc ON cc.entity_id = nt.id AND cc.n_campaigns = 1
LEFT JOIN already_removed ar ON ar.entity_interact_id = nt.interact_id
WHERE ar.entity_interact_id IS NULL
ORDER BY nt.email_norm
