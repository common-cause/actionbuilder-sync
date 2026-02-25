WITH ep_qualifiers AS (
  -- People who completed EP shifts in 2024
  SELECT
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
    CAST(events_6m AS INT64) as events_6m,
    CAST(phone_bank_dials AS INT64) as phone_bank_dials,
    'EP Shift 2024' as qualification_reason,
    LOWER(TRIM(email)) as email_normalized,
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(phone_number, ''), r'^\+', ''), r'^1', ''), r'[^\d]', '') as phone_normalized
  FROM ep_archive.ep_internal
  WHERE shifted_2024 = 'Y'
    AND (email IS NOT NULL OR phone_number IS NOT NULL)
),

mobilize_qualifiers AS (
  -- People who attended mobilize events in past year, with contact info from participations
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
  WHERE mp.attended = True
    AND (mp.utc_start_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
         OR mp.utc_override_start_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
    AND mp.user__email_address IS NOT NULL
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

all_qualifiers AS (
  -- Combine all qualification sources
  SELECT * FROM ep_qualifiers
  UNION ALL
  SELECT * FROM mobilize_qualifiers
  UNION ALL
  SELECT * FROM scaletowin_qualifiers
  UNION ALL
  SELECT * FROM action_network_qualifiers
  UNION ALL
  SELECT * FROM newmode_qualifiers
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
      NULLIF(MAX(CASE WHEN qualification_reason = 'EP Shift 2024' AND first_name IS NOT NULL THEN first_name END), ''),
      NULLIF(MAX(CASE WHEN first_name IS NOT NULL THEN first_name END), '')
    ) as first_name,

    COALESCE(
      NULLIF(MAX(CASE WHEN qualification_reason = 'EP Shift 2024' AND last_name IS NOT NULL THEN last_name END), ''),
      NULLIF(MAX(CASE WHEN last_name IS NOT NULL THEN last_name END), '')
    ) as last_name,

    -- Best phone (EP, then ScaleToWin, then others)
    COALESCE(
      NULLIF(MAX(CASE WHEN qualification_reason = 'EP Shift 2024' AND phone_number IS NOT NULL THEN phone_number END), ''),
      NULLIF(MAX(CASE WHEN qualification_reason = 'ScaleToWin Phone Bank' AND phone_number IS NOT NULL THEN phone_number END), ''),
      NULLIF(MAX(CASE WHEN phone_number IS NOT NULL THEN phone_number END), '')
    ) as phone_number,

    -- Best email (EP, then Action Network, then Mobilize, then NewMode)
    COALESCE(
      NULLIF(MAX(CASE WHEN qualification_reason = 'EP Shift 2024' AND email IS NOT NULL THEN email END), ''),
      NULLIF(MAX(CASE WHEN qualification_reason = 'Action Network 5+ Actions' AND email IS NOT NULL THEN email END), ''),
      NULLIF(MAX(CASE WHEN qualification_reason = 'Mobilize Event Past Year' AND email IS NOT NULL THEN email END), ''),
      NULLIF(MAX(CASE WHEN qualification_reason = 'NewMode Submission' AND email IS NOT NULL THEN email END), ''),
      NULLIF(MAX(CASE WHEN email IS NOT NULL THEN email END), '')
    ) as email,

    -- Geographic data (primarily from EP and Action Network)
    COALESCE(
      NULLIF(MAX(CASE WHEN qualification_reason = 'EP Shift 2024' AND state IS NOT NULL THEN state END), ''),
      NULLIF(MAX(CASE WHEN qualification_reason = 'Action Network 5+ Actions' AND state IS NOT NULL THEN state END), ''),
      NULLIF(MAX(CASE WHEN state IS NOT NULL THEN state END), '')
    ) as state,

    COALESCE(
      NULLIF(MAX(CASE WHEN qualification_reason = 'EP Shift 2024' AND county IS NOT NULL THEN county END), ''),
      NULLIF(MAX(CASE WHEN county IS NOT NULL THEN county END), '')
    ) as county,

    COALESCE(
      NULLIF(MAX(CASE WHEN qualification_reason = 'EP Shift 2024' AND zip_code IS NOT NULL THEN zip_code END), ''),
      NULLIF(MAX(CASE WHEN qualification_reason = 'Action Network 5+ Actions' AND zip_code IS NOT NULL THEN zip_code END), ''),
      NULLIF(MAX(CASE WHEN qualification_reason = 'Mobilize Event Past Year' AND zip_code IS NOT NULL THEN zip_code END), ''),
      NULLIF(MAX(CASE WHEN zip_code IS NOT NULL THEN zip_code END), '')
    ) as zip_code,

    -- Metadata
    COALESCE(
      NULLIF(MAX(CASE WHEN qualification_reason = 'EP Shift 2024' AND source_code IS NOT NULL THEN source_code END), ''),
      NULLIF(MAX(CASE WHEN source_code IS NOT NULL THEN source_code END), '')
    ) as source_code,

    COALESCE(
      MAX(CASE WHEN qualification_reason = 'EP Shift 2024' AND created_at IS NOT NULL THEN created_at END),
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
    COALESCE(SUM(an6.total_actions_6_months), 0) as action_network_actions

  FROM person_unified_contacts uc
  LEFT JOIN core_enhanced.enh_activistpools__emails epe
    ON uc.person_id = epe.person_id
  LEFT JOIN {{ ref('action_network_6mo_actions') }} an6
    ON LOWER(TRIM(epe.email)) = an6.email_normalized
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
    COALESCE(an6.total_actions_6_months, 0) as action_network_actions

  FROM unmatched_contacts um
  LEFT JOIN {{ ref('action_network_6mo_actions') }} an6
    ON LOWER(TRIM(um.email)) = an6.email_normalized
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
    fan.action_network_actions

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
