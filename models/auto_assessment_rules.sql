-- auto_assessment_rules: computes recommended assessment level per entity+campaign
--
-- Level 1 (any one of):
--   - Any Mobilize attendance (events_6m >= 1 or any all-time attendance)
--   - Any NewMode submission
--   - Any ScaleToWin call
--   - 20+ AN actions in past 6 months
--
-- Level 2 (any one of):
--   - 2+ ScaleToWin calls
--   - 2+ digital Mobilize events (is_virtual = TRUE)
--   - Any in-person Common Cause Mobilize event (organization_id = 6600)
--
-- Write policy (enforced here, not in sync script):
--   - Only upgrade, never downgrade
--   - Never overwrite assessments set by human organizers (created_by_id != 3)
--   - Safe to overwrite: no assessment, level=0, or level=1 set by created_by_id=3

WITH all_entity_emails AS (
  SELECT
    owner_id as entity_id,
    LOWER(TRIM(email)) as email_normalized
  FROM actionbuilder_cleaned.cln_actionbuilder__emails
  WHERE owner_type = 'Entity'
    AND status IN ('verified', 'user_added')
    AND email IS NOT NULL
),

all_entity_phones AS (
  SELECT
    owner_id as entity_id,
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(number, r'^\+', ''), r'^1', ''), r'[^\d]', '') as number_normalized
  FROM actionbuilder_cleaned.cln_actionbuilder__phone_numbers
  WHERE owner_type = 'Entity'
    AND status IN ('verified', 'user_added')
    AND number IS NOT NULL
    AND LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(number, r'^\+', ''), r'^1', ''), r'[^\d]', '')) = 10
),

-- Mobilize: total events 6m, digital events 6m, any in-person CC event
mobilize_by_entity AS (
  SELECT
    aee.entity_id,
    COUNT(DISTINCT CASE
      WHEN DATE(COALESCE(p.utc_override_start_date, p.utc_start_date)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
      THEN p.event_id
    END) as mobilize_events_6m,
    COUNT(DISTINCT CASE
      WHEN DATE(COALESCE(p.utc_override_start_date, p.utc_start_date)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
        AND e.is_virtual = TRUE
      THEN p.event_id
    END) as digital_mobilize_events_6m,
    COUNT(DISTINCT CASE
      WHEN e.organization_id = 6600 AND e.is_virtual = FALSE
      THEN p.event_id
    END) as in_person_cc_events_all_time,
    COUNT(DISTINCT p.event_id) as mobilize_events_all_time
  FROM all_entity_emails aee
  INNER JOIN mobilize_cleaned.cln_mobilize__participations p
    ON aee.email_normalized = LOWER(TRIM(COALESCE(p.user__email_address, p.email_at_signup)))
  INNER JOIN mobilize_cleaned.cln_mobilize__events e
    ON p.event_id = e.id
  WHERE p.status NOT IN ('CANCELLED')
    AND COALESCE(p.utc_override_start_date, p.utc_start_date) IS NOT NULL
    AND DATE(COALESCE(p.utc_override_start_date, p.utc_start_date)) <= CURRENT_DATE()
  GROUP BY aee.entity_id
),

-- AN actions in past 6 months
an_by_entity AS (
  SELECT
    aee.entity_id,
    SUM(an6.total_actions_6_months) as an_actions_6m
  FROM all_entity_emails aee
  INNER JOIN {{ ref('action_network_6mo_actions') }} an6
    ON aee.email_normalized = an6.email_normalized
  GROUP BY aee.entity_id
),

-- NewMode submissions (any = qualifies)
newmode_by_entity AS (
  SELECT
    aee.entity_id,
    SUM(nma.newmode_submission_count) as newmode_submissions
  FROM all_entity_emails aee
  INNER JOIN {{ ref('newmode_actions') }} nma
    ON aee.email_normalized = nma.email_normalized
  GROUP BY aee.entity_id
),

-- ScaleToWin calls
stw_by_entity AS (
  SELECT
    aep.entity_id,
    SUM(scd.phone_bank_calls_made) as stw_calls
  FROM all_entity_phones aep
  INNER JOIN {{ ref('scaletowin_call_data') }} scd
    ON aep.number_normalized = scd.caller_phone_number
  GROUP BY aep.entity_id
),

-- Entities in active campaigns
entities_in_campaigns AS (
  SELECT
    ce.entity_id,
    ce.campaign_id,
    c.interact_id as campaign_interact_id
  FROM actionbuilder_cleaned.cln_actionbuilder__campaigns_entities ce
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns c
    ON ce.campaign_id = c.id
  WHERE c.status = 'active'
    AND c.name != 'Test'
),

-- Current assessment: latest row per entity+campaign
current_assessments AS (
  SELECT entity_id, campaign_id, level, created_by_id
  FROM (
    SELECT
      owner_id as entity_id, campaign_id, level, created_by_id,
      ROW_NUMBER() OVER (
        PARTITION BY owner_id, campaign_id
        ORDER BY utc_updated_at DESC
      ) as rn
    FROM actionbuilder_cleaned.cln_actionbuilder__assessments
    WHERE owner_type = 'Entity'
  )
  WHERE rn = 1
),

-- Compute recommended level per entity
entity_levels AS (
  SELECT
    eic.entity_id,
    eic.campaign_id,
    eic.campaign_interact_id,

    -- Level 2 qualification flags
    COALESCE(stw.stw_calls, 0) >= 2 as l2_stw_calls,
    COALESCE(mob.digital_mobilize_events_6m, 0) >= 2 as l2_digital_mobilize,
    COALESCE(mob.in_person_cc_events_all_time, 0) >= 1 as l2_in_person_cc,

    -- Level 1 qualification flags
    COALESCE(mob.mobilize_events_6m, 0) >= 1
      OR COALESCE(mob.mobilize_events_all_time, 0) >= 1 as l1_mobilize,
    COALESCE(nmo.newmode_submissions, 0) >= 1 as l1_newmode,
    COALESCE(stw.stw_calls, 0) >= 1 as l1_stw,
    COALESCE(an.an_actions_6m, 0) >= 20 as l1_an_actions,

    -- Current assessment state
    ca.level as current_level,
    ca.created_by_id as current_created_by_id

  FROM entities_in_campaigns eic
  LEFT JOIN mobilize_by_entity mob ON mob.entity_id = eic.entity_id
  LEFT JOIN an_by_entity an ON an.entity_id = eic.entity_id
  LEFT JOIN newmode_by_entity nmo ON nmo.entity_id = eic.entity_id
  LEFT JOIN stw_by_entity stw ON stw.entity_id = eic.entity_id
  LEFT JOIN current_assessments ca
    ON ca.entity_id = eic.entity_id AND ca.campaign_id = eic.campaign_id
),

recommended AS (
  SELECT
    entity_id,
    campaign_id,
    campaign_interact_id,
    current_level,
    current_created_by_id,

    -- Recommended level
    CASE
      WHEN l2_stw_calls OR l2_digital_mobilize OR l2_in_person_cc THEN 2
      WHEN l1_mobilize OR l1_newmode OR l1_stw OR l1_an_actions THEN 1
      ELSE 0
    END as recommended_level,

    -- Qualification reasons (for debugging)
    ARRAY_TO_STRING(ARRAY_CONCAT(
      IF(l2_stw_calls, ['2+ STW calls'], []),
      IF(l2_digital_mobilize, ['2+ digital Mobilize'], []),
      IF(l2_in_person_cc, ['In-person CC event'], []),
      IF(l1_mobilize, ['Mobilize attendance'], []),
      IF(l1_newmode, ['NewMode submission'], []),
      IF(l1_stw, ['STW call'], []),
      IF(l1_an_actions, ['20+ AN actions'], [])
    ), ', ') as qualification_reasons

  FROM entity_levels
),

entity_interact_ids AS (
  SELECT id as entity_id_int, interact_id as entity_interact_id
  FROM actionbuilder_cleaned.cln_actionbuilder__entities
)

SELECT
  r.campaign_id,
  r.campaign_interact_id,
  eii.entity_interact_id as entity_id,
  r.recommended_level,
  COALESCE(r.current_level, 0) as current_level,
  r.current_created_by_id,
  r.qualification_reasons,

  -- Write policy: should we actually write this?
  CASE
    -- No existing assessment — safe to write
    WHEN r.current_level IS NULL THEN TRUE
    -- Current is 0 — safe to write
    WHEN r.current_level = 0 THEN TRUE
    -- Current is level 1 set by API user (id=3) — safe to upgrade
    WHEN r.current_level = 1 AND r.current_created_by_id = 3 THEN TRUE
    -- Current was set by a human organizer — do not touch
    ELSE FALSE
  END as should_write

FROM recommended r
LEFT JOIN entity_interact_ids eii ON eii.entity_id_int = r.entity_id
WHERE r.recommended_level > COALESCE(r.current_level, 0)
ORDER BY r.campaign_id, r.entity_id
