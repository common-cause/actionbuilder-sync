-- Automated assessment level recommendations for AB entities.
--
-- Write policy (enforced here — entities that fail are excluded):
--   - Only write if: no existing assessment, level=0, or level=1 AND created_by_id=3 (Rob/API)
--   - Never write to entities where a human organizer set level >= 1
--   - Only upgrade, never downgrade (suggested_level > current_level)
--
-- Level 1 criteria (any one of):
--   - Any Mobilize event attendance (all-time)
--   - Any NewMode submission
--   - Any ScaleToWin phone bank call
--   - 20+ AN actions in past 6 months
--
-- Level 2 criteria (any one of):
--   - 2+ ScaleToWin calls
--   - 2+ digital (virtual) Mobilize event attendances (all-time)
--   - Any in-person Common Cause Mobilize event (organization_id = 6600)
--
-- Output: one row per entity needing an assessment write.
-- Sync operation: apply_assessments in scripts/sync.py

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

-- Current assessments for write-policy check
latest_assessments AS (
  SELECT
    owner_id as entity_id,
    level,
    created_by_id,
    ROW_NUMBER() OVER (PARTITION BY owner_id ORDER BY utc_created_at DESC) as rn
  FROM actionbuilder_cleaned.cln_actionbuilder__assessments
  WHERE owner_type = 'Entity'
),

current_assessments AS (
  SELECT entity_id, level, created_by_id
  FROM latest_assessments
  WHERE rn = 1
),

-- Write-policy filter: entities we are allowed to assess
write_eligible_entities AS (
  SELECT e.id as entity_id
  FROM actionbuilder_cleaned.cln_actionbuilder__entities e
  LEFT JOIN current_assessments ca ON e.id = ca.entity_id
  WHERE ca.entity_id IS NULL                           -- no assessment yet
    OR ca.level = 0                                    -- level 0 (unassessed)
    OR (ca.level = 1 AND ca.created_by_id = 3)         -- level 1 from Rob/API, safe to overwrite
),

-- Level 1: any Mobilize attendance (all-time)
mobilize_any AS (
  SELECT DISTINCT aee.entity_id
  FROM all_entity_emails aee
  INNER JOIN mobilize_cleaned.cln_mobilize__participations mp
    ON aee.email_normalized = LOWER(TRIM(mp.user__email_address))
  WHERE mp.attended = True
),

-- Level 1: any NewMode submission
newmode_any AS (
  SELECT DISTINCT aee.entity_id
  FROM all_entity_emails aee
  INNER JOIN newmode_cleaned.cln_newmode__submissions nm
    ON aee.email_normalized = LOWER(TRIM(nm.contact_email))
  WHERE nm.testmode IS DISTINCT FROM TRUE
),

-- Level 1: any ScaleToWin call
stw_any AS (
  SELECT DISTINCT aep.entity_id
  FROM all_entity_phones aep
  INNER JOIN {{ ref('scaletowin_call_data') }} scd
    ON aep.number_normalized = scd.caller_phone_number
  WHERE scd.phone_bank_calls_made >= 1
),

-- Level 1: 20+ AN actions in past 6 months
an_20plus AS (
  SELECT DISTINCT aee.entity_id
  FROM all_entity_emails aee
  INNER JOIN {{ ref('action_network_6mo_actions') }} an6
    ON aee.email_normalized = an6.email_normalized
  WHERE an6.total_actions_6_months >= 20
),

-- Level 2: 2+ ScaleToWin calls
stw_repeated AS (
  SELECT DISTINCT aep.entity_id
  FROM all_entity_phones aep
  INNER JOIN {{ ref('scaletowin_call_data') }} scd
    ON aep.number_normalized = scd.caller_phone_number
  WHERE scd.phone_bank_calls_made >= 2
),

-- Level 2: 2+ digital Mobilize events attended (all-time)
digital_mobilize_counts AS (
  SELECT
    aee.entity_id,
    COUNT(DISTINCT mp.event_id) as digital_event_count
  FROM all_entity_emails aee
  INNER JOIN mobilize_cleaned.cln_mobilize__participations mp
    ON aee.email_normalized = LOWER(TRIM(mp.user__email_address))
  INNER JOIN mobilize_cleaned.cln_mobilize__events me
    ON mp.event_id = me.id
  WHERE mp.attended = True
    AND me.is_virtual = True
  GROUP BY aee.entity_id
),

digital_mobilize_2plus AS (
  SELECT entity_id
  FROM digital_mobilize_counts
  WHERE digital_event_count >= 2
),

-- Level 2: any in-person Common Cause Mobilize event (org_id 6600)
inperson_cc_mobilize AS (
  SELECT DISTINCT aee.entity_id
  FROM all_entity_emails aee
  INNER JOIN mobilize_cleaned.cln_mobilize__participations mp
    ON aee.email_normalized = LOWER(TRIM(mp.user__email_address))
  INNER JOIN mobilize_cleaned.cln_mobilize__events me
    ON mp.event_id = me.id
  WHERE mp.attended = True
    AND me.is_virtual = False
    AND me.organization_id = 6600
),

-- Campaign lookup (one active non-Test campaign per entity)
entity_campaigns AS (
  SELECT DISTINCT
    ce.entity_id,
    c.interact_id as campaign_interact_id
  FROM actionbuilder_cleaned.cln_actionbuilder__campaigns_entities ce
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns c
    ON ce.campaign_id = c.id
  WHERE c.status = 'active'
    AND c.name != 'Test'
),

-- Compute suggested level for each write-eligible entity
assessment_candidates AS (
  SELECT
    we.entity_id,
    ca.level as current_level,
    ca.created_by_id as current_created_by_id,

    CASE
      WHEN (stw_r.entity_id IS NOT NULL
            OR dm2.entity_id IS NOT NULL
            OR ipcm.entity_id IS NOT NULL)
        THEN 2
      WHEN (mob.entity_id IS NOT NULL
            OR nm.entity_id IS NOT NULL
            OR stw.entity_id IS NOT NULL
            OR an20.entity_id IS NOT NULL)
        THEN 1
      ELSE NULL
    END as suggested_level,

    -- Most specific qualifying reason (for logging)
    CASE
      WHEN stw_r.entity_id IS NOT NULL  THEN '2+ STW calls'
      WHEN dm2.entity_id IS NOT NULL    THEN '2+ digital Mobilize events'
      WHEN ipcm.entity_id IS NOT NULL   THEN 'In-person CC Mobilize event'
      WHEN mob.entity_id IS NOT NULL    THEN 'Mobilize attendance'
      WHEN nm.entity_id IS NOT NULL     THEN 'NewMode submission'
      WHEN stw.entity_id IS NOT NULL    THEN 'ScaleToWin call'
      WHEN an20.entity_id IS NOT NULL   THEN '20+ AN actions'
      ELSE NULL
    END as qualification_reason

  FROM write_eligible_entities we
  LEFT JOIN current_assessments ca   ON we.entity_id = ca.entity_id
  LEFT JOIN mobilize_any mob         ON we.entity_id = mob.entity_id
  LEFT JOIN newmode_any nm           ON we.entity_id = nm.entity_id
  LEFT JOIN stw_any stw              ON we.entity_id = stw.entity_id
  LEFT JOIN an_20plus an20           ON we.entity_id = an20.entity_id
  LEFT JOIN stw_repeated stw_r       ON we.entity_id = stw_r.entity_id
  LEFT JOIN digital_mobilize_2plus dm2  ON we.entity_id = dm2.entity_id
  LEFT JOIN inperson_cc_mobilize ipcm   ON we.entity_id = ipcm.entity_id
)

SELECT
  ac.entity_id,
  (SELECT e.interact_id
   FROM actionbuilder_cleaned.cln_actionbuilder__entities e
   WHERE e.id = ac.entity_id) as entity_interact_id,
  ec.campaign_interact_id,
  ac.current_level,
  ac.suggested_level,
  ac.qualification_reason

FROM assessment_candidates ac
INNER JOIN entity_campaigns ec ON ac.entity_id = ec.entity_id  -- must be in an active campaign

WHERE ac.suggested_level IS NOT NULL
  AND (ac.current_level IS NULL OR ac.suggested_level > ac.current_level)  -- only upgrade

ORDER BY ac.suggested_level DESC, ac.entity_id
