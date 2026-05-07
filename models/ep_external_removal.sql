-- ep_external_removal: entities to remove from AB because they were loaded
-- only via the "EP shift as decisive qualifier" path for a partner-org-coded
-- volunteer who also has no other CC touchpoint.
--
-- Background: prior to 2026-05-07, master_load_qualifiers loaded any EP
-- volunteer whose ep_archive.ep_internal row had shifted_2024='Y', regardless
-- of source_code. Partner-org volunteers (LWVMA, MassVOTE, ACLUM, kos,
-- dailykos, LCR, etc.) were intended to qualify only when they ALSO had
-- independent CC engagement; that rule was never enforced in code, so ~371
-- partner-org-coded shifted volunteers got loaded across 12 states.
--
-- The current rule (master_load_qualifiers): partner-org-coded EP shifters
-- are admitted when they also have at least one non-EP CC engagement in the
-- past 5 years (Mobilize event we owned, AN action, NewMode submission).
-- "Already-our-volunteer" override: if we've heard from someone through our
-- own channels, EP shift is corroboration, not poaching.
--
-- This view targets entities that fail that test:
--   1. Currently in any active state campaign
--   2. Email matches a partner-org EP source code (external_ep_emails)
--   3. Email is in ep_internal with shifted_2024='Y' (was loaded for EP shift)
--   4. Email is NOT in the current master_load_qualifiers — i.e., would not
--      be re-loaded today, including not via the cc_engaged_emails carve-out.
--
-- Output columns mirror dedup_candidates so a sync.py operation can consume
-- this view with the same delete_person flow.
--
-- This is a one-shot cleanup. Reprieves under the new rule remain in AB.

WITH external_ep_emails AS (
  SELECT DISTINCT LOWER(TRIM(fa.email)) AS email_norm
  FROM ep_archive.full_archive fa
  INNER JOIN ep_archive.source_codes sc
    ON LOWER(fa.source_code) = LOWER(sc.source_code)
  WHERE sc.external = 'Y'
    AND fa.email IS NOT NULL
),

ep_shifted_emails AS (
  SELECT DISTINCT LOWER(TRIM(email)) AS email_norm
  FROM ep_archive.ep_internal
  WHERE shifted_2024 = 'Y' AND email IS NOT NULL
),

still_qualified_emails AS (
  -- Anyone still in master_load_qualifiers under the post-fix rules.
  -- Partner-org volunteers in this set keep their AB record.
  SELECT DISTINCT LOWER(TRIM(email)) AS email_norm
  FROM {{ ref('master_load_qualifiers') }}
  WHERE email IS NOT NULL
),

state_to_campaign AS (
  -- Active state campaigns (matches dedup logic; excludes Test campaign)
  SELECT
    s.abbreviation AS state_abbr,
    c.id           AS campaign_id,
    c.interact_id  AS campaign_interact_id
  FROM actionnetwork_views.states s
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns c ON c.name = s.name
  WHERE c.status = 'active' AND c.name != 'Test'

  UNION DISTINCT
  -- Temporary: VA and DC campaigns (BQ replication lag)
  SELECT 'DC', 25, '3a227511-fd6f-40f6-abfc-4f2c05ff3b91'
  UNION DISTINCT
  SELECT 'VA', 24, '261251df-8836-4f90-a9fb-fdd5dc1798b1'
),

candidate_entities AS (
  -- Entities currently in a state campaign whose email is partner-org-coded
  -- AND was loaded for an EP shift AND no longer independently qualifies.
  SELECT DISTINCT
    e.id                     AS entity_id,
    e.interact_id            AS delete_interact_id,
    e.first_name             AS delete_first_name,
    e.last_name              AS delete_last_name,
    stc.campaign_interact_id,
    stc.state_abbr
  FROM actionbuilder_cleaned.cln_actionbuilder__entities e
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__emails ab_email
    ON ab_email.owner_id = e.id AND ab_email.owner_type = 'Entity'
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns_entities ce
    ON ce.entity_id = e.id
  INNER JOIN state_to_campaign stc
    ON ce.campaign_id = stc.campaign_id
  INNER JOIN external_ep_emails ext
    ON LOWER(TRIM(ab_email.email)) = ext.email_norm
  INNER JOIN ep_shifted_emails sy
    ON LOWER(TRIM(ab_email.email)) = sy.email_norm
  LEFT JOIN still_qualified_emails sq
    ON LOWER(TRIM(ab_email.email)) = sq.email_norm
  WHERE sq.email_norm IS NULL
    AND ab_email.email IS NOT NULL
)

SELECT
  delete_interact_id,
  delete_first_name,
  delete_last_name,
  campaign_interact_id,
  state_abbr,
  'ep_external_winding_route' AS removal_reason
FROM candidate_entities
