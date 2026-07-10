-- Materialized as a TABLE (not a view): this is the deep, NTL-joined tree that every heavy
-- downstream re-inlines (deduplicated_names_to_load, ep/mobilize_external_removal, the OT feeds).
-- As a view it pushed those consumers past BigQuery's query-planner ceiling ("too many
-- subqueries"). As a table they read flat stored rows instead. REQUIRES the nightly to run dbt
-- first (civis/run_dbt.sh, workflow step 0) so this table reflects current platform data — a
-- table is only as fresh as the last dbt run.
{{ config(materialized='table') }}

-- Emails of EP volunteers whose source code points to a coalition partner
-- (LWVMA, MassVOTE, ACLUM, kos, dailykos, LCR, etc.) per external_ptv_source_codes
-- (the canonical, case-collision-robust external-code set).
-- Anti-poaching rule: a partner-org code by itself is not enough to make
-- someone a CC volunteer. EP shift alone, or EP-org Mobilize attendance alone,
-- does not qualify these emails for AB load.
WITH external_ep_emails AS (
  SELECT DISTINCT LOWER(TRIM(fa.email)) AS email_norm
  FROM ep_archive.full_archive fa
  INNER JOIN {{ ref('external_ptv_source_codes') }} esc
    ON LOWER(fa.source_code) = esc.source_code
  WHERE fa.email IS NOT NULL
),

-- Emails of EP volunteers whose source code points to a CC/internal channel
-- (i.e. NOT a coalition partner) per ep_archive.source_codes. A CC-coded PTV
-- record is genuine CC engagement — it counts as an independent, non-Mobilize
-- touch in the Rule A rescue set below.
cc_coded_ep_emails AS (
  SELECT DISTINCT LOWER(TRIM(fa.email)) AS email_norm
  FROM ep_archive.full_archive fa
  WHERE fa.email IS NOT NULL
    AND fa.source_code IS NOT NULL
    AND LOWER(fa.source_code) NOT IN (SELECT source_code FROM {{ ref('external_ptv_source_codes') }})
),

-- "Other group" PTV source codes, lowercased — used to test the Mobilize source
-- code (referrer__utm_source). A Mobilize signup carrying one of these codes is a
-- coalition partner's recruitment leaking through a shared Mobilize feed, not ours.
external_source_codes AS (
  SELECT source_code FROM {{ ref('external_ptv_source_codes') }}
),

-- Emails that are ACTIVELY subscribed to at least one CC Action Network group
-- (subscription_statuses.status = 1). Used to discount "unsubbed AN records":
-- an unsubscribed AN presence (status != 1) never counts as CC engagement.
subscribed_an_emails AS (
  SELECT DISTINCT LOWER(TRIM(REGEXP_REPLACE(u.email, r'^"(.*)"$', r'\1'))) AS email_norm
  FROM actionnetwork_cleaned.cln_actionnetwork__users u
  INNER JOIN actionnetwork_cleaned.cln_actionnetwork__subscription_statuses ss
    ON ss.subscriber_id = u.id
  WHERE ss.status = 1
    AND u.email IS NOT NULL
),

-- Independent, NON-Mobilize CC engagement in the past 5 years (plus all-time
-- Soapboxx, a recent platform). The AN branch is gated on subscribed_an_emails so
-- an unsubscribed AN record does not count. Mobilize is deliberately absent: this
-- set is reused as the Rule A rescue, where a Mobilize candidate must not rescue
-- itself with the very Mobilize appearance under scrutiny.
non_mobilize_online_touch AS (
  SELECT DISTINCT LOWER(TRIM(REGEXP_REPLACE(ana.email, r'^"(.*)"$', r'\1'))) AS email_norm
  FROM {{ ref('action_network_actions') }} ana
  INNER JOIN subscribed_an_emails sae
    ON LOWER(TRIM(REGEXP_REPLACE(ana.email, r'^"(.*)"$', r'\1'))) = sae.email_norm
  WHERE ana.email IS NOT NULL
    AND ana.actions_all_time >= 1
    AND DATE(ana.latest_action_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)

  UNION DISTINCT

  SELECT DISTINCT LOWER(TRIM(nm.contact_email)) AS email_norm
  FROM newmode_cleaned.cln_newmode__submissions nm
  WHERE nm.contact_email IS NOT NULL
    AND nm.testmode IS DISTINCT FROM TRUE
    AND DATE(nm.utc_created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)

  UNION DISTINCT

  -- Soapboxx is a recent platform (all data within the 5-year window); any story
  -- is genuine CC engagement.
  SELECT DISTINCT sbx.email_normalized AS email_norm
  FROM {{ ref('soapboxx_stories') }} sbx
  WHERE sbx.email_normalized IS NOT NULL
),

-- Emails with any real, non-EP engagement with a CC system in the past 5 years.
-- "Already-our-volunteer" override on the anti-poaching rule: if someone
-- partner-org-coded has independently shown up in a CC channel — a Mobilize
-- event we owned, a (subscribed) AN action, a NewMode submission, a Soapboxx
-- story — they're already in our relationship, and EP shift adds confirmation
-- rather than serving as the decisive qualifier.
-- Election Protection-organized Mobilize events are excluded here: that's EP
-- activity in a Mobilize wrapper, not "another CC system".
-- ScaleToWin is keyed by phone, not email, so it isn't represented here;
-- that's a known small gap.
cc_engaged_emails AS (
  SELECT DISTINCT LOWER(TRIM(mp.user__email_address)) AS email_norm
  FROM mobilize_cleaned.cln_mobilize__participations mp
  WHERE mp.attended = TRUE
    AND mp.user__email_address IS NOT NULL
    AND COALESCE(mp.organization__name, '') != 'Election Protection'
    AND (DATE(mp.utc_start_date)          >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)
         OR DATE(mp.utc_override_start_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR))

  UNION DISTINCT

  -- Subscribed AN / NewMode / Soapboxx (the unsubbed-AN exclusion applies here too).
  SELECT email_norm FROM non_mobilize_online_touch
),

-- Rule A rescue set: an independent CC touch that lets a partner-org-coded person
-- (external PTV source code) be claimed when they appear in Mobilize. Subscribed
-- AN / NewMode / Soapboxx, or a CC-coded PTV record. Deliberately excludes Mobilize
-- — a bare Mobilize appearance, even at a CC event, is not enough on its own.
rule_a_rescue_emails AS (
  SELECT email_norm FROM non_mobilize_online_touch
  UNION DISTINCT
  SELECT email_norm FROM cc_coded_ep_emails
),

-- Phone-keyed Rule A rescue: a ScaleToWin phone-bank shift is genuine CC
-- engagement, but ScaleToWin has no email, so it can't live in the email-keyed
-- rescue set. Normalized to match mobilize_qualifiers.phone_normalized.
scaletowin_rescue_phones AS (
  SELECT DISTINCT
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(scd.caller_phone_number, ''), r'^\+', ''), r'^1', ''), r'[^\d]', '') AS phone_norm
  FROM {{ ref('scaletowin_call_data') }} scd
  WHERE scd.phone_bank_calls_made > 0
    AND scd.caller_phone_number IS NOT NULL
),

ep_qualifiers AS (
  -- People who completed EP shifts in 2024.
  -- Partner-org-coded emails (external_ep_emails) are admitted only when they
  -- also appear in cc_engaged_emails — i.e., already-our-volunteer override.
  SELECT
    ep.first_name,
    ep.last_name,
    ep.phone_number,
    ep.email,
    ep.state,
    ep.county,
    ep.zip_code,
    ep.source_code,
    ep.created_at,
    ep.shifted_2024,
    CAST(ep.events_6m AS INT64) as events_6m,
    CAST(ep.phone_bank_dials AS INT64) as phone_bank_dials,
    'EP Shift 2024' as qualification_reason,
    LOWER(TRIM(ep.email)) as email_normalized,
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(ep.phone_number, ''), r'^\+', ''), r'^1', ''), r'[^\d]', '') as phone_normalized
  FROM ep_archive.ep_internal ep
  LEFT JOIN external_ep_emails ext
    ON LOWER(TRIM(ep.email)) = ext.email_norm
  LEFT JOIN cc_engaged_emails cce
    ON LOWER(TRIM(ep.email)) = cce.email_norm
  WHERE ep.shifted_2024 = 'Y'
    AND (ep.email IS NOT NULL OR ep.phone_number IS NOT NULL)
    AND (ext.email_norm IS NULL OR cce.email_norm IS NOT NULL)
),

mobilize_qualifiers AS (
  -- People who attended mobilize events in past year, with contact info from participations.
  -- For partner-org-coded EP volunteers, attendance at Election Protection-owned
  -- events doesn't count as Mobilize qualification — that's EP activity in a
  -- Mobilize wrapper, not "another CC system". Their attendance at non-EP-org
  -- events does count, both directly here and as the cc_engaged_emails signal
  -- that admits them through the EP qualifier.
  SELECT DISTINCT
    mp.user__given_name as first_name,
    mp.user__family_name as last_name,
    mp.user__phone_number as phone_number,
    mp.user__email_address as email,
    CAST(NULL AS STRING) as state,
    CAST(NULL AS STRING) as county,
    mp.user__postal_code as zip_code,
    CAST(NULL AS STRING) as source_code,
    mp.utc_created_date as created_at,
    CAST(NULL AS STRING) as shifted_2024,
    CAST(NULL AS INT64) as events_6m,
    CAST(NULL AS INT64) as phone_bank_dials,
    'Mobilize Event Past Year' as qualification_reason,
    LOWER(TRIM(mp.user__email_address)) as email_normalized,
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(mp.user__phone_number, ''), r'^\+', ''), r'^1', ''), r'[^\d]', '') as phone_normalized
  FROM mobilize_cleaned.cln_mobilize__participations mp
  LEFT JOIN external_ep_emails ext
    ON LOWER(TRIM(mp.user__email_address)) = ext.email_norm
  -- Rule B: the Mobilize source code (referrer__utm_source) matched against
  -- "other group" PTV source codes.
  LEFT JOIN external_source_codes esc
    ON LOWER(mp.referrer__utm_source) = esc.source_code
  -- Rule A: independent non-Mobilize CC touch (email-keyed) ...
  LEFT JOIN rule_a_rescue_emails rar
    ON LOWER(TRIM(mp.user__email_address)) = rar.email_norm
  -- ... and (phone-keyed) ScaleToWin shift.
  LEFT JOIN scaletowin_rescue_phones srp
    ON REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(mp.user__phone_number, ''), r'^\+', ''), r'^1', ''), r'[^\d]', '') = srp.phone_norm
   AND REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(mp.user__phone_number, ''), r'^\+', ''), r'^1', ''), r'[^\d]', '') != ''
  WHERE mp.attended = True
    AND (mp.utc_start_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
         OR mp.utc_override_start_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
    AND mp.user__email_address IS NOT NULL
    -- Existing: EP-org Mobilize attendance doesn't count for partner-org-coded people.
    AND NOT (
      ext.email_norm IS NOT NULL
      AND mp.organization__name = 'Election Protection'
    )
    -- Rule B: drop signups whose source code is an "other group" PTV code. This is
    -- that group's recruitment through a shared Mobilize feed, not ours. OFP is exempt
    -- — OFP attendees qualify via the separate ofp_qualifiers branch regardless.
    AND esc.source_code IS NULL
    -- Rule A: a partner-org-coded person (external PTV source code) is claimed via
    -- Mobilize only if they ALSO have an independent, non-Mobilize CC touch
    -- (subscribed AN / NewMode / Soapboxx / CC-coded PTV / ScaleToWin). A bare
    -- Mobilize appearance — even at a CC event — isn't enough, and an unsubbed AN
    -- record never rescues them.
    AND NOT (
      ext.email_norm IS NOT NULL
      AND rar.email_norm IS NULL
      AND srp.phone_norm IS NULL
    )
),

scaletowin_qualifiers AS (
  -- People who completed phone bank dials
  SELECT DISTINCT
    CAST(NULL AS STRING) as first_name,
    CAST(NULL AS STRING) as last_name,
    scd.caller_phone_number as phone_number,
    CAST(NULL AS STRING) as email,
    CAST(NULL AS STRING) as state,
    CAST(NULL AS STRING) as county,
    CAST(NULL AS STRING) as zip_code,
    CAST(NULL AS STRING) as source_code,
    CAST(NULL AS TIMESTAMP) as created_at,
    CAST(NULL AS STRING) as shifted_2024,
    CAST(NULL AS INT64) as events_6m,
    CAST(scd.phone_bank_calls_made AS INT64) as phone_bank_dials,
    'ScaleToWin Phone Bank' as qualification_reason,
    CAST(NULL AS STRING) as email_normalized,
    scd.caller_phone_number as phone_normalized
  FROM {{ ref('scaletowin_call_data') }} scd
  WHERE scd.phone_bank_calls_made > 0
    AND scd.caller_phone_number IS NOT NULL
),

action_network_qualifiers AS (
  -- People meeting the per-state AN action threshold (default 20; MI/NE = 5)
  SELECT DISTINCT
    COALESCE(cf.first_name, CAST(NULL AS STRING)) as first_name,
    COALESCE(cf.last_name, CAST(NULL AS STRING)) as last_name,
    COALESCE(cf.phone_number, CAST(NULL AS STRING)) as phone_number,
    TRIM(REGEXP_REPLACE(u.email, r'^"(.*)"$', r'\1')) as email,
    COALESCE(cf.state, CAST(NULL AS STRING)) as state,
    CAST(NULL AS STRING) as county,
    COALESCE(cf.zip_code, CAST(NULL AS STRING)) as zip_code,
    CAST(NULL AS STRING) as source_code,
    u.utc_created_at as created_at,
    CAST(NULL AS STRING) as shifted_2024,
    CAST(NULL AS INT64) as events_6m,
    CAST(NULL AS INT64) as phone_bank_dials,
    'Action Network 5+ Actions' as qualification_reason,
    LOWER(TRIM(REGEXP_REPLACE(u.email, r'^"(.*)"$', r'\1'))) as email_normalized,
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(cf.phone_number, ''), r'^\+', ''), r'^1', ''), r'[^\d]', '') as phone_normalized
  FROM {{ ref('action_network_6mo_actions') }} an6
  INNER JOIN actionnetwork_cleaned.cln_actionnetwork__users u
    ON an6.user_id = u.id
  LEFT JOIN actionnetwork_cleaned.cln_actionnetwork__core_fields cf
    ON u.id = cf.user_id
  LEFT JOIN {{ ref('state_an_thresholds') }} sat
    ON cf.state = sat.state
  WHERE an6.total_actions_6_months >= COALESCE(sat.min_an_actions_6m, 20)
    AND an6.email_normalized IS NOT NULL
),

newmode_qualifiers AS (
  -- People who submitted any NewMode action (1 submission = qualifies, same as Mobilize)
  SELECT DISTINCT
    CAST(NULL AS STRING) as first_name,
    CAST(NULL AS STRING) as last_name,
    CAST(NULL AS STRING) as phone_number,
    nm.contact_email as email,
    CAST(NULL AS STRING) as state,
    CAST(NULL AS STRING) as county,
    CAST(NULL AS STRING) as zip_code,
    CAST(NULL AS STRING) as source_code,
    nm.utc_created_at as created_at,
    CAST(NULL AS STRING) as shifted_2024,
    CAST(NULL AS INT64) as events_6m,
    CAST(NULL AS INT64) as phone_bank_dials,
    'NewMode Submission' as qualification_reason,
    LOWER(TRIM(nm.contact_email)) as email_normalized,
    CAST(NULL AS STRING) as phone_normalized
  FROM newmode_cleaned.cln_newmode__submissions nm
  WHERE nm.contact_email IS NOT NULL
    AND nm.testmode IS DISTINCT FROM TRUE
),

soapboxx_qualifiers AS (
  -- People who submitted any Soapboxx story (1 submission = qualifies, like
  -- NewMode/Mobilize). High-effort signal: a recorded video/photo/written testimonial.
  SELECT DISTINCT
    sbx.first_name as first_name,
    sbx.last_name as last_name,
    sbx.phone as phone_number,
    sbx.email_normalized as email,
    CAST(NULL AS STRING) as state,
    CAST(NULL AS STRING) as county,
    sbx.zip_code as zip_code,
    CAST(NULL AS STRING) as source_code,
    CAST(NULL AS TIMESTAMP) as created_at,
    CAST(NULL AS STRING) as shifted_2024,
    CAST(NULL AS INT64) as events_6m,
    CAST(NULL AS INT64) as phone_bank_dials,
    'Soapboxx Story' as qualification_reason,
    sbx.email_normalized as email_normalized,
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(sbx.phone, ''), r'^\+', ''), r'^1', ''), r'[^\d]', '') as phone_normalized
  FROM {{ ref('soapboxx_stories') }} sbx
  WHERE sbx.soapboxx_stories > 0
    AND sbx.email_normalized IS NOT NULL
),

ofp_qualifiers AS (
  -- Organizing for Power training attendees (Mobilize event 907019 timeslots via the
  -- ofp_training_map seed). OFP is a deliberate CC training program, so attendance
  -- qualifies someone for AB load on its own — all-time, NOT subject to the rolling
  -- 365-day Mobilize window (mirrors the "any submission qualifies" pattern of
  -- newmode/soapboxx). No anti-poaching gate: attending our OFP training is direct CC
  -- engagement. Sourced from ofp_universe (collapsed to one row per attendee).
  SELECT DISTINCT
    first_name,
    last_name,
    phone_number,
    email_normalized as email,
    state,                          -- zip-derived (see ofp_universe); routes to state campaign
    CAST(NULL AS STRING) as county,
    zip_code,
    CAST(NULL AS STRING) as source_code,
    CAST(NULL AS TIMESTAMP) as created_at,
    CAST(NULL AS STRING) as shifted_2024,
    CAST(NULL AS INT64) as events_6m,
    CAST(NULL AS INT64) as phone_bank_dials,
    'OFP Training' as qualification_reason,
    email_normalized,
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(phone_number, ''), r'^\+', ''), r'^1', ''), r'[^\d]', '') as phone_normalized
  FROM {{ ref('ofp_universe') }}
),

-- ============================================================
-- Additional EP shift cycles: 2022 and 2026.
-- The 2024 branch (ep_qualifiers) reads the purpose-built ep_internal.shifted_2024
-- flag. Those cycles have no equivalent flag, so each is derived from its own
-- shifting-tool feed and enriched/gated for parity with 2024. 2025 is intentionally
-- out of scope (no EP shift program / data located as of 2026-07-10).
-- ============================================================

-- Non-org provenance / platform markers that are mis-flagged external='Y' in the
-- hand-maintained ep_archive.source_codes, and therefore leak into the shared
-- external_ptv_source_codes set. They are NOT coalition partners, so treating them
-- as "poaching" wrongly suppresses real CC volunteers:
--   previous_years — carried over from CC's own prior-year EP roster (largest code)
--   actionnetwork  — the Action Network platform, not an organization
--   vf             — voter-file provenance
--   youth          — youth-program provenance
-- Per decision 2026-07-10 this override is scoped to the 2022 & 2026 branches ONLY;
-- the shared model and the live 2024 load / ep_/mobilize_external_removal feeds are
-- deliberately left as-is. Fold these into external_ptv_source_codes if the call is
-- ever made to correct the gate globally.
provenance_override_codes AS (
  SELECT code FROM UNNEST(['previous_years', 'actionnetwork', 'vf', 'youth']) AS code
),

-- The canonical external set minus the provenance overrides above.
external_ptv_codes_strict AS (
  SELECT source_code
  FROM {{ ref('external_ptv_source_codes') }}
  WHERE source_code NOT IN (SELECT code FROM provenance_override_codes)
),

-- Emails whose EP archive source code is a genuine coalition partner (provenance
-- markers excluded). Parallel to external_ep_emails but strict; used to anti-poach
-- the 2022 & 2026 branches.
external_ep_emails_strict AS (
  SELECT DISTINCT LOWER(TRIM(fa.email)) AS email_norm
  FROM ep_archive.full_archive fa
  INNER JOIN external_ptv_codes_strict esc
    ON LOWER(fa.source_code) = esc.source_code
  WHERE fa.email IS NOT NULL
),

-- One row per email from the EP archive (all cycles), used to enrich 2022 shift
-- workers (who arrive with only an email) with name / phone / geo / source code.
-- Prefer a complete, most-recent record.
ep_full_archive_dedup AS (
  SELECT
    email_norm, first_name, last_name, phone_number, email,
    state, county, zip_code, source_code, created_at
  FROM (
    SELECT
      LOWER(TRIM(email)) AS email_norm,
      first_name, last_name, phone_number, email,
      state, county, zip_code, source_code, created_at,
      ROW_NUMBER() OVER (
        PARTITION BY LOWER(TRIM(email))
        ORDER BY
          CASE WHEN first_name IS NOT NULL AND last_name IS NOT NULL THEN 0 ELSE 1 END,
          created_at DESC
      ) AS rn
    FROM ep_archive.full_archive
    WHERE email IS NOT NULL
  )
  WHERE rn = 1
),

-- Resolved "real" source per email: the historical data's answer to the vague
-- `previous_years` stamp. Prefer a genuine (non-provenance) source code over the
-- provenance markers, most-recent first; fall back to whatever exists. This is a
-- core reason the EP archive/legacy data was loaded — recovering the true origin for
-- people the feed only marked `previous_years`. NOTE: source_code is carried through
-- the load feed but is NOT written to ActionBuilder, so this is informational; the
-- external/internal gate itself already resolves via external_ep_emails_strict
-- (which ignores the provenance markers and keys on genuine partner codes).
ep_resolved_source AS (
  SELECT email_norm, source_code AS resolved_source_code
  FROM (
    SELECT
      LOWER(TRIM(email)) AS email_norm,
      source_code,
      ROW_NUMBER() OVER (
        PARTITION BY LOWER(TRIM(email))
        ORDER BY
          CASE WHEN LOWER(source_code) IN (SELECT code FROM provenance_override_codes) THEN 1 ELSE 0 END,
          created_at DESC
      ) AS rn
    FROM ep_archive.full_archive
    WHERE email IS NOT NULL AND source_code IS NOT NULL
  )
  WHERE rn = 1
),

-- Distinct volunteers who booked an EP shift in the 2022 cycle (one row per
-- volunteer-shift signup in the shifting tool). Presence here = "actually shifted",
-- matching the strictness of shifted_2024='Y'.
ep_2022_shift_vols AS (
  SELECT
    LOWER(TRIM(volunteer_email)) AS email_norm,
    ANY_VALUE(volunteer_email)   AS email_raw
  FROM ep_2023.shifting_tool_shifts
  WHERE volunteer_email IS NOT NULL
    AND volunteer_email LIKE '%@%'
  GROUP BY LOWER(TRIM(volunteer_email))
),

ep_2022_qualifiers AS (
  -- 2022 EP shift workers, enriched from the archive and gated exactly like the 2024
  -- branch: a partner-org-coded email (external_ep_emails_strict) is admitted only if
  -- it also has an independent CC touch (cc_engaged_emails).
  SELECT
    fa.first_name,
    fa.last_name,
    fa.phone_number,
    COALESCE(fa.email, sv.email_raw) AS email,
    fa.state,
    fa.county,
    fa.zip_code,
    COALESCE(rs.resolved_source_code, fa.source_code) as source_code,
    fa.created_at,
    CAST(NULL AS STRING) as shifted_2024,
    CAST(NULL AS INT64)  as events_6m,
    CAST(NULL AS INT64)  as phone_bank_dials,
    'EP Shift 2022' as qualification_reason,
    sv.email_norm as email_normalized,
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(fa.phone_number, ''), r'^\+', ''), r'^1', ''), r'[^\d]', '') as phone_normalized
  FROM ep_2022_shift_vols sv
  LEFT JOIN ep_full_archive_dedup fa
    ON sv.email_norm = fa.email_norm
  LEFT JOIN ep_resolved_source rs
    ON sv.email_norm = rs.email_norm
  LEFT JOIN external_ep_emails_strict ext
    ON sv.email_norm = ext.email_norm
  LEFT JOIN cc_engaged_emails cce
    ON sv.email_norm = cce.email_norm
  WHERE (ext.email_norm IS NULL OR cce.email_norm IS NOT NULL)
),

-- Person-level rollup of the live 2026 PTV shifting-tool feed. n_external / n_internal
-- classify each person by the source codes across their shifts (provenance markers
-- count as internal, per external_ptv_codes_strict).
ep_2026_shift_person AS (
  SELECT
    LOWER(TRIM(sv.email))      AS email_norm,
    MAX(sv.first_name)         AS first_name,
    MAX(sv.last_name)          AS last_name,
    MAX(sv.phone_number)       AS phone_number,
    MAX(sv.email)              AS email,
    MAX(sv.state)              AS state,
    MAX(sv.county)             AS county,
    MAX(NULLIF(sv.source, '')) AS source_code,
    CAST(MIN(sv.date) AS TIMESTAMP) AS created_at,
    COUNTIF(esc.source_code IS NOT NULL) AS n_external,
    COUNTIF(sv.source IS NOT NULL AND sv.source != '' AND esc.source_code IS NULL) AS n_internal
  FROM ptv_raw_2026.shift_volunteers sv
  LEFT JOIN external_ptv_codes_strict esc
    ON LOWER(sv.source) = esc.source_code
  WHERE sv.email IS NOT NULL
    AND sv.email LIKE '%@%'
  GROUP BY LOWER(TRIM(sv.email))
),

ep_2026_qualifiers AS (
  -- 2026 EP shift workers. External if every coded shift points to a partner org
  -- (n_external>0 AND n_internal=0) OR the email is archive-external; admitted anyway
  -- when there's an independent CC touch (cc_engaged_emails). No zip in the feed.
  SELECT
    pp.first_name,
    pp.last_name,
    pp.phone_number,
    pp.email,
    pp.state,
    pp.county,
    CAST(NULL AS STRING) as zip_code,
    -- Keep a genuine current-cycle code; only fall back to the resolved historical
    -- source when the 2026 feed left a provenance marker or nothing.
    CASE
      WHEN pp.source_code IS NULL
        OR LOWER(pp.source_code) IN (SELECT code FROM provenance_override_codes)
      THEN COALESCE(rs.resolved_source_code, pp.source_code)
      ELSE pp.source_code
    END as source_code,
    pp.created_at,
    CAST(NULL AS STRING) as shifted_2024,
    CAST(NULL AS INT64)  as events_6m,
    CAST(NULL AS INT64)  as phone_bank_dials,
    'EP Shift 2026' as qualification_reason,
    pp.email_norm as email_normalized,
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(pp.phone_number, ''), r'^\+', ''), r'^1', ''), r'[^\d]', '') as phone_normalized
  FROM ep_2026_shift_person pp
  LEFT JOIN ep_resolved_source rs
    ON pp.email_norm = rs.email_norm
  LEFT JOIN external_ep_emails_strict ext
    ON pp.email_norm = ext.email_norm
  LEFT JOIN cc_engaged_emails cce
    ON pp.email_norm = cce.email_norm
  WHERE (
    NOT ((pp.n_external > 0 AND pp.n_internal = 0) OR ext.email_norm IS NOT NULL)
    OR cce.email_norm IS NOT NULL
  )
),

all_qualifiers AS (
  -- Combine all qualification sources
  SELECT * FROM ep_qualifiers
  UNION ALL
  SELECT * FROM ep_2022_qualifiers
  UNION ALL
  SELECT * FROM ep_2026_qualifiers
  UNION ALL
  SELECT * FROM mobilize_qualifiers
  UNION ALL
  SELECT * FROM scaletowin_qualifiers
  UNION ALL
  SELECT * FROM action_network_qualifiers
  UNION ALL
  SELECT * FROM newmode_qualifiers
  UNION ALL
  SELECT * FROM soapboxx_qualifiers
  UNION ALL
  SELECT * FROM ofp_qualifiers
),

qualifiers_with_person_ids AS (
  -- Map qualifiers to person_ids via core_enhanced tables
  SELECT
    aq.*,
    -- Get person_id via email matching
    COALESCE(epe.person_id, ppp.person_id) as person_id,
    COALESCE(epe.original_person_id, ppp.original_person_id) as original_person_id,
    CASE
      WHEN epe.person_id IS NOT NULL THEN 'email'
      WHEN ppp.person_id IS NOT NULL THEN 'phone'
      ELSE 'no_match'
    END as person_match_method
  FROM all_qualifiers aq
  LEFT JOIN core_enhanced.enh_activistpools__emails epe
    ON aq.email_normalized = LOWER(TRIM(epe.email))
  LEFT JOIN core_enhanced.enh_activistpools__phones ppp
    ON aq.phone_normalized = REGEXP_REPLACE(ppp.phone_number, r'[^\d]', '')
    AND aq.phone_normalized IS NOT NULL
    AND aq.phone_normalized != ''
),

person_unified_contacts AS (
  -- Unify contacts by person_id, keeping best info from each source
  SELECT
    person_id,
    original_person_id,

    -- Best contact info prioritizing completeness and EP data
    COALESCE(
      NULLIF(MAX(CASE WHEN qualification_reason LIKE 'EP Shift %' AND first_name IS NOT NULL THEN first_name END), ''),
      NULLIF(MAX(CASE WHEN first_name IS NOT NULL THEN first_name END), '')
    ) as first_name,

    COALESCE(
      NULLIF(MAX(CASE WHEN qualification_reason LIKE 'EP Shift %' AND last_name IS NOT NULL THEN last_name END), ''),
      NULLIF(MAX(CASE WHEN last_name IS NOT NULL THEN last_name END), '')
    ) as last_name,

    -- Best phone (EP, then ScaleToWin, then others)
    COALESCE(
      NULLIF(MAX(CASE WHEN qualification_reason LIKE 'EP Shift %' AND phone_number IS NOT NULL THEN phone_number END), ''),
      NULLIF(MAX(CASE WHEN qualification_reason = 'ScaleToWin Phone Bank' AND phone_number IS NOT NULL THEN phone_number END), ''),
      NULLIF(MAX(CASE WHEN phone_number IS NOT NULL THEN phone_number END), '')
    ) as phone_number,

    -- Best email (EP, then Action Network, then Mobilize, then NewMode)
    COALESCE(
      NULLIF(MAX(CASE WHEN qualification_reason LIKE 'EP Shift %' AND email IS NOT NULL THEN email END), ''),
      NULLIF(MAX(CASE WHEN qualification_reason = 'Action Network 5+ Actions' AND email IS NOT NULL THEN email END), ''),
      NULLIF(MAX(CASE WHEN qualification_reason = 'Mobilize Event Past Year' AND email IS NOT NULL THEN email END), ''),
      NULLIF(MAX(CASE WHEN qualification_reason = 'NewMode Submission' AND email IS NOT NULL THEN email END), ''),
      NULLIF(MAX(CASE WHEN email IS NOT NULL THEN email END), '')
    ) as email,

    -- Geographic data (primarily from EP and Action Network)
    COALESCE(
      NULLIF(MAX(CASE WHEN qualification_reason LIKE 'EP Shift %' AND state IS NOT NULL THEN state END), ''),
      NULLIF(MAX(CASE WHEN qualification_reason = 'Action Network 5+ Actions' AND state IS NOT NULL THEN state END), ''),
      NULLIF(MAX(CASE WHEN state IS NOT NULL THEN state END), '')
    ) as state,

    COALESCE(
      NULLIF(MAX(CASE WHEN qualification_reason LIKE 'EP Shift %' AND county IS NOT NULL THEN county END), ''),
      NULLIF(MAX(CASE WHEN county IS NOT NULL THEN county END), '')
    ) as county,

    COALESCE(
      NULLIF(MAX(CASE WHEN qualification_reason LIKE 'EP Shift %' AND zip_code IS NOT NULL THEN zip_code END), ''),
      NULLIF(MAX(CASE WHEN qualification_reason = 'Action Network 5+ Actions' AND zip_code IS NOT NULL THEN zip_code END), ''),
      NULLIF(MAX(CASE WHEN qualification_reason = 'Mobilize Event Past Year' AND zip_code IS NOT NULL THEN zip_code END), ''),
      NULLIF(MAX(CASE WHEN zip_code IS NOT NULL THEN zip_code END), '')
    ) as zip_code,

    -- Metadata
    COALESCE(
      NULLIF(MAX(CASE WHEN qualification_reason LIKE 'EP Shift %' AND source_code IS NOT NULL THEN source_code END), ''),
      NULLIF(MAX(CASE WHEN source_code IS NOT NULL THEN source_code END), '')
    ) as source_code,

    COALESCE(
      MAX(CASE WHEN qualification_reason LIKE 'EP Shift %' AND created_at IS NOT NULL THEN created_at END),
      MAX(CASE WHEN created_at IS NOT NULL THEN created_at END)
    ) as created_at,

    -- Activity data (take maximum values)
    MAX(shifted_2024) as shifted_2024,
    MAX(events_6m) as events_6m,
    MAX(phone_bank_dials) as phone_bank_dials,

    -- Qualification tracking
    STRING_AGG(DISTINCT qualification_reason, ', ' ORDER BY qualification_reason) as qualification_reasons,
    COUNT(DISTINCT qualification_reason) as qualification_count

  FROM qualifiers_with_person_ids
  WHERE person_id IS NOT NULL  -- Only include people we can match to core_enhanced
  GROUP BY person_id, original_person_id
),

unmatched_contacts AS (
  -- Handle people who don't exist in core_enhanced tables
  SELECT
    CAST(NULL AS STRING) as person_id,
    CAST(NULL AS STRING) as original_person_id,
    first_name,
    last_name,
    phone_number,
    email,
    state,
    county,
    zip_code,
    source_code,
    created_at,
    shifted_2024,
    events_6m,
    phone_bank_dials,
    qualification_reason as qualification_reasons,
    1 as qualification_count
  FROM qualifiers_with_person_ids
  WHERE person_id IS NULL
),

final_with_action_network AS (
  -- Add Action Network data and field names for both unified and unmatched
  SELECT
    uc.person_id,
    uc.first_name,
    uc.last_name,
    uc.phone_number,
    uc.email,
    uc.state,
    uc.county,
    uc.zip_code,
    uc.source_code,
    uc.created_at,
    uc.shifted_2024,
    uc.events_6m,
    uc.phone_bank_dials,
    uc.qualification_reasons,
    uc.qualification_count,

    -- Get Action Network actions count via person_id
    COALESCE(SUM(an6.total_actions_6_months), 0) as action_network_actions,

    -- Get Soapboxx story count via person_id
    COALESCE(SUM(sbx.soapboxx_stories), 0) as soapboxx_stories

  FROM person_unified_contacts uc
  LEFT JOIN core_enhanced.enh_activistpools__emails epe
    ON uc.person_id = epe.person_id
  LEFT JOIN {{ ref('action_network_6mo_actions') }} an6
    ON LOWER(TRIM(epe.email)) = an6.email_normalized
  LEFT JOIN {{ ref('soapboxx_stories') }} sbx
    ON LOWER(TRIM(epe.email)) = sbx.email_normalized
  GROUP BY uc.person_id, uc.first_name, uc.last_name, uc.phone_number, uc.email,
           uc.state, uc.county, uc.zip_code, uc.source_code, uc.created_at,
           uc.shifted_2024, uc.events_6m, uc.phone_bank_dials, uc.qualification_reasons, uc.qualification_count

  UNION ALL

  -- Include unmatched contacts with their Action Network data
  SELECT
    um.person_id,
    um.first_name,
    um.last_name,
    um.phone_number,
    um.email,
    um.state,
    um.county,
    um.zip_code,
    um.source_code,
    um.created_at,
    um.shifted_2024,
    um.events_6m,
    um.phone_bank_dials,
    um.qualification_reasons,
    um.qualification_count,

    -- Get Action Network actions directly for unmatched
    COALESCE(an6.total_actions_6_months, 0) as action_network_actions,

    -- Get Soapboxx story count directly for unmatched
    COALESCE(sbx.soapboxx_stories, 0) as soapboxx_stories

  FROM unmatched_contacts um
  LEFT JOIN {{ ref('action_network_6mo_actions') }} an6
    ON LOWER(TRIM(um.email)) = an6.email_normalized
  LEFT JOIN {{ ref('soapboxx_stories') }} sbx
    ON LOWER(TRIM(um.email)) = sbx.email_normalized
),

final_with_voter_file_fallback AS (
  -- Add voter file data as fallback for missing contact information
  SELECT
    fan.person_id,

    -- Use voter file data as fallback for missing names
    COALESCE(
      NULLIF(fan.first_name, ''),
      INITCAP(LOWER(ts.vb_tsmart_first_name))
    ) as first_name,

    COALESCE(
      NULLIF(fan.last_name, ''),
      INITCAP(LOWER(ts.vb_tsmart_last_name))
    ) as last_name,

    -- Use voter file phone as fallback
    COALESCE(
      NULLIF(fan.phone_number, ''),
      ts.vb_voterbase_phone
    ) as phone_number,

    fan.email, -- Keep original email (voter file doesn't have emails)

    -- Use voter file geographic data as fallback
    COALESCE(
      NULLIF(fan.state, ''),
      ts.vb_tsmart_state
    ) as state,

    fan.county, -- Keep original county (voter file county handling is complex)

    COALESCE(
      NULLIF(fan.zip_code, ''),
      ts.vb_tsmart_zip
    ) as zip_code,

    fan.source_code,
    fan.created_at,
    fan.shifted_2024,
    fan.events_6m,
    fan.phone_bank_dials,
    fan.qualification_reasons,
    fan.qualification_count,
    fan.action_network_actions,
    fan.soapboxx_stories

  FROM final_with_action_network fan
  LEFT JOIN core_targetsmart_enhanced.enh_activistpools__identities ident
    ON fan.person_id = ident.person_id
  LEFT JOIN targetsmart_enhanced.enh_targetsmart__ntl_current ts
    ON ident.voterbase_id = ts.vb_voterbase_id
)

SELECT DISTINCT
  person_id,
  first_name,
  last_name,
  phone_number,
  email,
  state,
  county,
  zip_code,
  source_code,
  created_at,
  shifted_2024,
  events_6m,
  phone_bank_dials,
  action_network_actions,
  soapboxx_stories,

  -- Add field name columns for upload interface
  'Action Network Actions' as action_network_field,
  'Events Attended Past 6 Months' as events_field,
  'Phone Bank Calls Made' as pb_field,
  'First Event Attended' as first_event_field,
  'Most Recent Event Attended' as mr_event_field,

  -- Add event dates (will need to join back to get these)
  CAST(NULL AS DATE) as first_event_date,
  CAST(NULL AS DATE) as mr_event_date,

  qualification_reasons,
  qualification_count

FROM final_with_voter_file_fallback
WHERE email IS NOT NULL OR phone_number IS NOT NULL
ORDER BY
  qualification_count DESC,
  CASE WHEN shifted_2024 = 'Y' THEN 1 ELSE 2 END,
  CASE WHEN action_network_actions >= 5 THEN 1 ELSE 2 END,
  last_name, first_name
