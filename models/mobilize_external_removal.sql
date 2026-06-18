-- mobilize_external_removal: entities to remove from AB because they represent
-- another coalition group's volunteers who entered via the Mobilize path and would
-- NOT be loaded under the anti-poaching rules added to master_load_qualifiers:
--   Rule A — a partner-org-coded person (external PTV source code) with no
--            independent, non-Mobilize CC touch.
--   Rule B — a person whose qualifying Mobilize signup(s) carry an "other group"
--            PTV source code (referrer__utm_source).
--
-- Precise scoping (avoids removing people who merely aged out of the window or
-- entered via another path): a candidate must have an IN-WINDOW (<=365d) attended
-- Mobilize signup that WOULD have qualified them under the OLD rules
-- (has_old_qualifying_signup), yet is absent from the current master_load_qualifiers
-- (still_qualified_emails). With an old-qualifying in-window signup present, the only
-- thing that can drop them from the new qualifiers is Rule A or Rule B — so no rescue
-- recomputation is needed; absence from master_load_qualifiers IS the signal.
--
-- Partitioned from ep_external_removal: that model owns external-PTV-coded EP shifters
-- (ep_shifted_emails); Condition X here excludes them. OFP attendees are exempt.
--
-- Output columns mirror ep_external_removal so remove_mobilize_externals in sync.py
-- can consume this view with the same delete_person flow.
--
-- This is a one-shot cleanup. Reprieves under the new rules remain in AB.

WITH external_ep_emails AS (
  SELECT DISTINCT LOWER(TRIM(fa.email)) AS email_norm
  FROM ep_archive.full_archive fa
  INNER JOIN {{ ref('external_ptv_source_codes') }} esc
    ON LOWER(fa.source_code) = esc.source_code
  WHERE fa.email IS NOT NULL
),

ep_shifted_emails AS (
  -- EP shifters are owned by ep_external_removal; partition them out of Condition X.
  SELECT DISTINCT LOWER(TRIM(email)) AS email_norm
  FROM ep_archive.ep_internal
  WHERE shifted_2024 = 'Y' AND email IS NOT NULL
),

external_source_codes AS (
  SELECT source_code FROM {{ ref('external_ptv_source_codes') }}
),

ofp_emails AS (
  -- OFP attendees are exempt — they are genuinely ours.
  SELECT DISTINCT LOWER(TRIM(email_normalized)) AS email_norm
  FROM {{ ref('ofp_universe') }}
  WHERE email_normalized IS NOT NULL
),

still_qualified_emails AS (
  -- Anyone still in master_load_qualifiers under the post-fix rules keeps their record.
  SELECT DISTINCT LOWER(TRIM(email)) AS email_norm
  FROM {{ ref('master_load_qualifiers') }}
  WHERE email IS NOT NULL
),

mob_window AS (
  -- Per-email summary of IN-WINDOW (<=365d) attended Mobilize signups:
  --   has_old_qualifying_signup — would have appeared in mobilize_qualifiers under the
  --     OLD rules (attended, in window, not an EP-org event for an external-PTV person)
  --   has_clean_signup    — at least one non-externally-source-coded signup
  --   has_external_signup — at least one externally-source-coded signup
  SELECT
    LOWER(TRIM(mp.user__email_address)) AS email_norm,
    LOGICAL_OR(
      NOT (ext.email_norm IS NOT NULL AND mp.organization__name = 'Election Protection')
    ) AS has_old_qualifying_signup,
    LOGICAL_OR(esc.source_code IS NULL)     AS has_clean_signup,
    LOGICAL_OR(esc.source_code IS NOT NULL) AS has_external_signup
  FROM mobilize_cleaned.cln_mobilize__participations mp
  LEFT JOIN external_source_codes esc
    ON LOWER(mp.referrer__utm_source) = esc.source_code
  LEFT JOIN external_ep_emails ext
    ON LOWER(TRIM(mp.user__email_address)) = ext.email_norm
  WHERE mp.attended = TRUE
    AND mp.user__email_address IS NOT NULL
    AND (mp.utc_start_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
         OR mp.utc_override_start_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
  GROUP BY 1
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
  -- Entities in a state campaign with an in-window old-qualifying Mobilize signup
  -- that no longer qualify (Rule A or Rule B is the cause) and are not OFP-exempt.
  SELECT DISTINCT
    e.id                     AS entity_id,
    e.interact_id            AS delete_interact_id,
    e.first_name             AS delete_first_name,
    e.last_name              AS delete_last_name,
    stc.campaign_interact_id,
    stc.state_abbr,
    CASE
      WHEN ext.email_norm IS NOT NULL THEN 'mobilize_external_partner_ptv'  -- Rule A
      ELSE 'mobilize_external_source_code'                                   -- Rule B
    END AS removal_reason
  FROM actionbuilder_cleaned.cln_actionbuilder__entities e
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__emails ab_email
    ON ab_email.owner_id = e.id AND ab_email.owner_type = 'Entity'
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns_entities ce
    ON ce.entity_id = e.id
  INNER JOIN state_to_campaign stc
    ON ce.campaign_id = stc.campaign_id
  INNER JOIN mob_window mw
    ON LOWER(TRIM(ab_email.email)) = mw.email_norm
   AND mw.has_old_qualifying_signup
  LEFT JOIN external_ep_emails ext
    ON LOWER(TRIM(ab_email.email)) = ext.email_norm
  LEFT JOIN ep_shifted_emails sh
    ON LOWER(TRIM(ab_email.email)) = sh.email_norm
  LEFT JOIN still_qualified_emails sq
    ON LOWER(TRIM(ab_email.email)) = sq.email_norm
  LEFT JOIN ofp_emails ofp
    ON LOWER(TRIM(ab_email.email)) = ofp.email_norm
  WHERE ab_email.email IS NOT NULL
    -- Would NOT be re-loaded today (with an old-qualifying in-window signup present,
    -- this isolates Rule A / Rule B as the cause).
    AND sq.email_norm IS NULL
    -- OFP attendees are exempt.
    AND ofp.email_norm IS NULL
    AND (
      -- Condition X (Rule A): partner-org-coded, but NOT an EP shifter
      -- (ep_external_removal owns the EP-shifter subset).
      (ext.email_norm IS NOT NULL AND sh.email_norm IS NULL)
      OR
      -- Condition Y (Rule B): not partner-coded, but the entire in-window Mobilize
      -- footprint is externally-source-coded (no clean signup).
      (ext.email_norm IS NULL AND mw.has_external_signup AND NOT mw.has_clean_signup)
    )
)

SELECT
  delete_interact_id,
  delete_first_name,
  delete_last_name,
  campaign_interact_id,
  state_abbr,
  removal_reason
FROM candidate_entities ce
-- Removal gap: drop entities already removed from this campaign per sync_log
-- (BQ campaigns_entities never reflects hard deletes — replication gap #1).
WHERE NOT EXISTS (
  SELECT 1 FROM {{ ref('removed_campaign_entities') }} r
  WHERE r.entity_interact_id = ce.delete_interact_id
    AND r.campaign_interact_id = ce.campaign_interact_id
)
