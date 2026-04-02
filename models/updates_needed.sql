WITH correct_values AS (
  -- Get the correct values from external data
  SELECT
    entity_id,
    events_attended_past_6_months,
    most_recent_event_attended,
    first_event_attended,
    action_network_actions_6mo,
    state_actions_6_months,
    top_state_action_taker_value,
    phone_bank_calls_made,
    newmode_actions,
    newmode_actions_value,
    top_national_action_taker_value,
    events_attended_past_6_months_value,
    most_recent_event_attended_value,
    first_event_attended_value,
    action_network_actions_6mo_value,
    state_actions_6mo_value,
    phone_bank_calls_made_value,
    events_6mo_sync_string,
    recent_event_sync_string,
    first_event_sync_string,
    action_network_6mo_sync_string,
    state_actions_6mo_sync_string,
    top_state_action_taker_sync_string,
    phone_calls_sync_string,
    newmode_actions_sync_string,
    top_national_action_taker_sync_string
  FROM {{ ref('correct_participation_values') }}
  WHERE has_participation_data = TRUE  -- Only entities with actual participation data
),

hot_prospect_entities AS (
  -- Entities currently on the Hot Prospect list (top 10 per campaign)
  SELECT DISTINCT entity_id
  FROM {{ ref('hot_prospects') }}
),

current_ab_values AS (
  -- Get current ActionBuilder values for our participation fields
  SELECT
    entity_id,
    campaign_id,
    tag_name,
    current_value,
    removal_string
  FROM {{ ref('current_tag_values') }}
  WHERE tag_name IN (
    'Events Attended Past 6 Months',
    'Most Recent Event Attended',
    'First Event Attended',
    'Action Network Actions',
    'Action Network State Actions',
    'Top State Action Taker',
    'Phone Bank Calls Made',
    'NewMode Actions',
    'Top National Action Network Activist',
    'Hot Prospect',
    'Organizing Basics',
    'Storytelling',
    'Relational Organizing',
    'Rapid Response Basics',
    'Leader',
    'Host',
    'Participant',
    'Total Conversations',
    'Host Prospect'
  )
),

pivot_current_values AS (
  -- Pivot current ActionBuilder values to one row per entity/campaign,
  -- including removal strings for each field
  SELECT
    entity_id,
    campaign_id,
    MAX(CASE WHEN tag_name = 'Events Attended Past 6 Months'       THEN current_value END) as current_events_6mo,
    MAX(CASE WHEN tag_name = 'Most Recent Event Attended'           THEN current_value END) as current_recent_event,
    MAX(CASE WHEN tag_name = 'First Event Attended'                 THEN current_value END) as current_first_event,
    MAX(CASE WHEN tag_name = 'Action Network Actions'               THEN current_value END) as current_action_network,
    MAX(CASE WHEN tag_name = 'Action Network State Actions'         THEN current_value END) as current_state_actions,
    MAX(CASE WHEN tag_name = 'Top State Action Taker'               THEN current_value END) as current_top_state_performer,
    MAX(CASE WHEN tag_name = 'Phone Bank Calls Made'                THEN current_value END) as current_phone_calls,
    MAX(CASE WHEN tag_name = 'NewMode Actions'                      THEN current_value END) as current_newmode_actions,
    MAX(CASE WHEN tag_name = 'Top National Action Network Activist' THEN current_value END) as current_national_top,
    MAX(CASE WHEN tag_name = 'Hot Prospect'                         THEN current_value END) as current_hot_prospect,

    -- Removal strings: tag-interact-id:|:tagging-interact-id
    MAX(CASE WHEN tag_name = 'Events Attended Past 6 Months'       THEN removal_string END) as removal_ids_events_6mo,
    MAX(CASE WHEN tag_name = 'Most Recent Event Attended'           THEN removal_string END) as removal_ids_recent_event,
    MAX(CASE WHEN tag_name = 'First Event Attended'                 THEN removal_string END) as removal_ids_first_event,
    MAX(CASE WHEN tag_name = 'Action Network Actions'               THEN removal_string END) as removal_ids_action_network,
    MAX(CASE WHEN tag_name = 'Action Network State Actions'         THEN removal_string END) as removal_ids_state_actions,
    MAX(CASE WHEN tag_name = 'Top State Action Taker'               THEN removal_string END) as removal_ids_top_state_performer,
    MAX(CASE WHEN tag_name = 'Phone Bank Calls Made'                THEN removal_string END) as removal_ids_phone_calls,
    MAX(CASE WHEN tag_name = 'NewMode Actions'                      THEN removal_string END) as removal_ids_newmode_actions,
    MAX(CASE WHEN tag_name = 'Top National Action Network Activist' THEN removal_string END) as removal_ids_national_top,
    MAX(CASE WHEN tag_name = 'Hot Prospect'                         THEN removal_string END) as removal_ids_hot_prospect
  FROM current_ab_values
  GROUP BY entity_id, campaign_id
),

entities_in_active_campaigns AS (
  -- Get all entities in active campaigns
  SELECT DISTINCT
    ce.entity_id,
    ce.campaign_id
  FROM actionbuilder_cleaned.cln_actionbuilder__campaigns_entities ce
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns c
    ON ce.campaign_id = c.id
  WHERE c.status = 'active'
),

value_comparisons AS (
  -- Compare current vs correct values - using LEFT JOIN from correct_values
  -- to ensure entities without current values are included
  SELECT
    eac.campaign_id,
    cv.entity_id,

    -- Current ActionBuilder values (normalized, will be NULL for entities without tags)
    COALESCE(pcv.current_events_6mo, '0')          as current_events_6mo,
    COALESCE(pcv.current_recent_event, '')          as current_recent_event,
    COALESCE(pcv.current_first_event, '')           as current_first_event,
    COALESCE(pcv.current_action_network, '0')       as current_action_network,
    COALESCE(pcv.current_state_actions, '0')        as current_state_actions,
    COALESCE(pcv.current_top_state_performer, '')   as current_top_state_performer,
    COALESCE(pcv.current_phone_calls, '0')          as current_phone_calls,
    COALESCE(pcv.current_newmode_actions, '0')      as current_newmode_actions,
    COALESCE(pcv.current_national_top, '')          as current_national_top,
    COALESCE(pcv.current_hot_prospect, '')          as current_hot_prospect,

    -- Correct values from external data
    cv.events_attended_past_6_months_value          as correct_events_6mo,
    COALESCE(cv.most_recent_event_attended_value, '') as correct_recent_event,
    COALESCE(cv.first_event_attended_value, '')     as correct_first_event,
    cv.action_network_actions_6mo_value             as correct_action_network,
    cv.state_actions_6mo_value                      as correct_state_actions,
    COALESCE(cv.top_state_action_taker_value, '')   as correct_top_state_performer,
    cv.phone_bank_calls_made_value                  as correct_phone_calls,
    cv.newmode_actions_value                        as correct_newmode_actions,
    COALESCE(cv.top_national_action_taker_value, '') as correct_national_top,
    -- Hot prospect correct value: 'Hot Prospect' if on list, '' if not
    CASE WHEN hp.entity_id IS NOT NULL THEN 'Hot Prospect' ELSE '' END as correct_hot_prospect,

    -- Sync strings for updates
    cv.events_6mo_sync_string,
    cv.recent_event_sync_string,
    cv.first_event_sync_string,
    cv.action_network_6mo_sync_string,
    cv.state_actions_6mo_sync_string,
    cv.top_state_action_taker_sync_string,
    cv.phone_calls_sync_string,
    cv.newmode_actions_sync_string,
    cv.top_national_action_taker_sync_string,
    -- Hot prospect sync string is built here (not stored in correct_participation_values)
    CASE WHEN hp.entity_id IS NOT NULL
      THEN 'Engagement:|:Prospect Identification:|:Hot Prospect:|:standard_response:Hot Prospect'
      ELSE NULL
    END as hot_prospect_sync_string,

    -- Removal strings (NULL when there is no existing value to remove)
    pcv.removal_ids_events_6mo,
    pcv.removal_ids_recent_event,
    pcv.removal_ids_first_event,
    pcv.removal_ids_action_network,
    pcv.removal_ids_state_actions,
    pcv.removal_ids_top_state_performer,
    pcv.removal_ids_phone_calls,
    pcv.removal_ids_newmode_actions,
    pcv.removal_ids_national_top,
    pcv.removal_ids_hot_prospect,

    -- Identify which fields need updates
    CASE
      WHEN COALESCE(pcv.current_events_6mo, '0') != cv.events_attended_past_6_months_value
      THEN TRUE ELSE FALSE
    END as events_6mo_needs_update,

    CASE
      WHEN COALESCE(pcv.current_recent_event, '') != COALESCE(cv.most_recent_event_attended_value, '')
      THEN TRUE ELSE FALSE
    END as recent_event_needs_update,

    CASE
      WHEN COALESCE(pcv.current_first_event, '') != COALESCE(cv.first_event_attended_value, '')
      THEN TRUE ELSE FALSE
    END as first_event_needs_update,

    CASE
      WHEN COALESCE(pcv.current_action_network, '0') != cv.action_network_actions_6mo_value
      THEN TRUE ELSE FALSE
    END as action_network_needs_update,

    CASE
      WHEN COALESCE(pcv.current_state_actions, '0') != cv.state_actions_6mo_value
      THEN TRUE ELSE FALSE
    END as state_actions_needs_update,

    CASE
      WHEN COALESCE(pcv.current_top_state_performer, '') != COALESCE(cv.top_state_action_taker_value, '')
      THEN TRUE ELSE FALSE
    END as top_state_performer_needs_update,

    CASE
      WHEN COALESCE(pcv.current_phone_calls, '0') != cv.phone_bank_calls_made_value
      THEN TRUE ELSE FALSE
    END as phone_calls_needs_update,

    CASE
      WHEN COALESCE(pcv.current_newmode_actions, '0') != cv.newmode_actions_value
      THEN TRUE ELSE FALSE
    END as newmode_needs_update,

    CASE
      WHEN COALESCE(pcv.current_national_top, '') != COALESCE(cv.top_national_action_taker_value, '')
      THEN TRUE ELSE FALSE
    END as national_top_needs_update,

    CASE
      WHEN COALESCE(pcv.current_hot_prospect, '') != (CASE WHEN hp.entity_id IS NOT NULL THEN 'Hot Prospect' ELSE '' END)
      THEN TRUE ELSE FALSE
    END as hot_prospect_needs_update

  FROM correct_values cv
  INNER JOIN entities_in_active_campaigns eac
    ON cv.entity_id = eac.entity_id
  LEFT JOIN pivot_current_values pcv
    ON cv.entity_id = pcv.entity_id
    AND eac.campaign_id = pcv.campaign_id
  LEFT JOIN hot_prospect_entities hp
    ON cv.entity_id = hp.entity_id
),

updates_to_apply AS (
  -- Generate individual update records for each field that needs updating
  SELECT
    campaign_id,
    entity_id,
    'Events Attended Past 6 Months' as field_name,
    'Event Attendance Summary' as field_group,
    events_6mo_sync_string as sync_string,
    current_events_6mo as current_value,
    correct_events_6mo as correct_value,
    removal_ids_events_6mo as removal_ids
  FROM value_comparisons
  WHERE events_6mo_needs_update = TRUE

  UNION ALL

  SELECT
    campaign_id,
    entity_id,
    'Most Recent Event Attended' as field_name,
    'Event Attendance History' as field_group,
    recent_event_sync_string as sync_string,
    current_recent_event as current_value,
    correct_recent_event as correct_value,
    removal_ids_recent_event as removal_ids
  FROM value_comparisons
  WHERE recent_event_needs_update = TRUE

  UNION ALL

  SELECT
    campaign_id,
    entity_id,
    'First Event Attended' as field_name,
    'Event Attendance History' as field_group,
    first_event_sync_string as sync_string,
    current_first_event as current_value,
    correct_first_event as correct_value,
    removal_ids_first_event as removal_ids
  FROM value_comparisons
  WHERE first_event_needs_update = TRUE

  UNION ALL

  SELECT
    campaign_id,
    entity_id,
    'Action Network Actions' as field_name,
    'Online Actions Past 6 Months' as field_group,
    action_network_6mo_sync_string as sync_string,
    current_action_network as current_value,
    correct_action_network as correct_value,
    removal_ids_action_network as removal_ids
  FROM value_comparisons
  WHERE action_network_needs_update = TRUE

  UNION ALL

  SELECT
    campaign_id,
    entity_id,
    'Action Network State Actions' as field_name,
    'Online Actions Past 6 Months' as field_group,
    state_actions_6mo_sync_string as sync_string,
    current_state_actions as current_value,
    correct_state_actions as correct_value,
    removal_ids_state_actions as removal_ids
  FROM value_comparisons
  WHERE state_actions_needs_update = TRUE

  UNION ALL

  SELECT
    campaign_id,
    entity_id,
    'Top State Action Taker' as field_name,
    'State Online Actions' as field_group,
    top_state_action_taker_sync_string as sync_string,
    current_top_state_performer as current_value,
    correct_top_state_performer as correct_value,
    removal_ids_top_state_performer as removal_ids
  FROM value_comparisons
  WHERE top_state_performer_needs_update = TRUE

  UNION ALL

  SELECT
    campaign_id,
    entity_id,
    'Phone Bank Calls Made' as field_name,
    'Event Attendance Summary' as field_group,
    phone_calls_sync_string as sync_string,
    current_phone_calls as current_value,
    correct_phone_calls as correct_value,
    removal_ids_phone_calls as removal_ids
  FROM value_comparisons
  WHERE phone_calls_needs_update = TRUE

  UNION ALL

  SELECT
    campaign_id,
    entity_id,
    'NewMode Actions' as field_name,
    'Online Actions Past 6 Months' as field_group,
    newmode_actions_sync_string as sync_string,
    current_newmode_actions as current_value,
    correct_newmode_actions as correct_value,
    removal_ids_newmode_actions as removal_ids
  FROM value_comparisons
  WHERE newmode_needs_update = TRUE

  UNION ALL

  SELECT
    campaign_id,
    entity_id,
    'Top National Action Network Activist' as field_name,
    'National Online Actions' as field_group,
    top_national_action_taker_sync_string as sync_string,
    current_national_top as current_value,
    correct_national_top as correct_value,
    removal_ids_national_top as removal_ids
  FROM value_comparisons
  WHERE national_top_needs_update = TRUE

  UNION ALL

  SELECT
    campaign_id,
    entity_id,
    'Hot Prospect' as field_name,
    'Prospect Engagement' as field_group,
    hot_prospect_sync_string as sync_string,
    current_hot_prospect as current_value,
    correct_hot_prospect as correct_value,
    removal_ids_hot_prospect as removal_ids
  FROM value_comparisons
  WHERE hot_prospect_needs_update = TRUE

  UNION ALL

  -- OFP training tags (additive multiselect — never removes, only adds missing)
  SELECT
    campaign_id,
    entity_id,
    field_name,
    field_group,
    sync_string,
    current_value,
    correct_value,
    removal_ids
  FROM {{ ref('ofp_attendance') }}

  UNION ALL

  -- 1MC Campaign Role tags (additive multiselect — never removes, only adds missing)
  SELECT
    campaign_id,
    entity_id,
    field_name,
    field_group,
    sync_string,
    current_value,
    correct_value,
    removal_ids
  FROM {{ ref('1mc_role_attendance') }}

  UNION ALL

  -- 1MC Total Conversations per Host (number, updates when value changes)
  SELECT
    campaign_id,
    entity_id,
    field_name,
    field_group,
    sync_string,
    current_value,
    correct_value,
    removal_ids
  FROM {{ ref('1mc_total_conversations') }}

  UNION ALL

  -- 1MC Participant tags (additive — never removes, only adds missing)
  SELECT
    campaign_id,
    entity_id,
    field_name,
    field_group,
    sync_string,
    current_value,
    correct_value,
    removal_ids
  FROM {{ ref('1mc_participants') }}

  UNION ALL

  -- 1MC Host Prospect tags (additive — never removes, only adds missing)
  SELECT
    campaign_id,
    entity_id,
    field_name,
    field_group,
    sync_string,
    current_value,
    correct_value,
    removal_ids
  FROM {{ ref('1mc_prospects') }}
),

entity_interact_ids AS (
  SELECT id as entity_id_int, interact_id as entity_interact_id
  FROM actionbuilder_cleaned.cln_actionbuilder__entities
),

entity_campaign_ids AS (
  SELECT
    ce.entity_id,
    STRING_AGG(c.interact_id, ',') as campaign_ids
  FROM actionbuilder_cleaned.cln_actionbuilder__campaigns c
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns_entities ce
    ON c.id = ce.campaign_id
  WHERE c.status = 'active'
  GROUP BY ce.entity_id
)

-- Final output: only records that need updates with separate columns per field group
SELECT
  campaign_id,
  eii.entity_interact_id as entity_id,
  field_name,

  -- _tag columns: value to add (NULL when there is nothing to add)
  CASE
    WHEN field_group = 'Event Attendance History' AND correct_value != ''
    THEN sync_string
    ELSE NULL
  END as event_participation_history_tag,
  CASE
    WHEN field_group = 'Event Attendance Summary' AND correct_value != ''
    THEN sync_string
    ELSE NULL
  END as event_participation_summary_tag,
  CASE
    WHEN field_group = 'Online Actions Past 6 Months' AND correct_value != ''
    THEN sync_string
    ELSE NULL
  END as online_actions_past_6_months_tag,
  CASE
    WHEN field_group = 'State Online Actions' AND correct_value != ''
    THEN sync_string
    ELSE NULL
  END as state_online_actions_tag,
  CASE
    WHEN field_group = 'National Online Actions' AND correct_value != ''
    THEN sync_string
    ELSE NULL
  END as national_online_actions_tag,
  CASE
    WHEN field_group = 'Prospect Engagement' AND correct_value != ''
    THEN sync_string
    ELSE NULL
  END as engagement_tag,
  CASE
    WHEN field_group = 'Organizing for Power' AND correct_value != ''
    THEN sync_string
    ELSE NULL
  END as ofp_tag,
  CASE
    WHEN field_group = 'Million Conversations Role' AND correct_value != ''
    THEN sync_string
    ELSE NULL
  END as million_conversations_role_tag,
  CASE
    WHEN field_group = 'Total Conversations' AND correct_value != ''
    THEN sync_string
    ELSE NULL
  END as million_conversations_activity_tag,
  CASE
    WHEN field_group = 'Million Conversations Prospect' AND correct_value != ''
    THEN sync_string
    ELSE NULL
  END as million_conversations_prospect_tag,

  -- _tag_remove columns: existing tagging to delete before adding new value
  -- Format: tag-interact-id:|:tagging-interact-id
  -- NULL when there is no existing value to remove (i.e. this is a first-time write)
  CASE
    WHEN field_group = 'Event Attendance History' AND current_value != ''
    THEN removal_ids
    ELSE NULL
  END as event_participation_history_tag_remove,
  CASE
    WHEN field_group = 'Event Attendance Summary' AND current_value != ''
    THEN removal_ids
    ELSE NULL
  END as event_participation_summary_tag_remove,
  CASE
    WHEN field_group = 'Online Actions Past 6 Months' AND current_value != ''
    THEN removal_ids
    ELSE NULL
  END as online_actions_past_6_months_tag_remove,
  CASE
    WHEN field_group = 'State Online Actions' AND current_value != ''
    THEN removal_ids
    ELSE NULL
  END as state_online_actions_tag_remove,
  CASE
    WHEN field_group = 'National Online Actions' AND current_value != ''
    THEN removal_ids
    ELSE NULL
  END as national_online_actions_tag_remove,
  CASE
    WHEN field_group = 'Prospect Engagement' AND current_value != ''
    THEN removal_ids
    ELSE NULL
  END as engagement_tag_remove,
  -- OFP is additive-only (multiselect); removal is always NULL
  CAST(NULL AS STRING) as ofp_tag_remove,
  -- 1MC Campaign Role is additive-only (multiselect); removal is always NULL
  CAST(NULL AS STRING) as million_conversations_role_tag_remove,
  -- 1MC Total Conversations: remove old value before writing new one
  CASE
    WHEN field_group = 'Total Conversations' AND current_value != '' AND current_value != '0'
    THEN removal_ids
    ELSE NULL
  END as million_conversations_activity_tag_remove,
  -- 1MC Prospect is additive-only; removal is always NULL
  CAST(NULL AS STRING) as million_conversations_prospect_tag_remove,

  current_value,
  correct_value,

  eci.campaign_ids,

  -- Add metadata for tracking
  CURRENT_TIMESTAMP() as comparison_timestamp,

  -- Change type classification
  CASE
    WHEN current_value = '' OR current_value = '0' AND correct_value != '' AND correct_value != '0' THEN 'New Value'
    WHEN current_value != '' AND current_value != '0' AND (correct_value = '' OR correct_value = '0') THEN 'Clear Value'
    WHEN current_value != '' AND correct_value != '' AND current_value != correct_value THEN 'Update Value'
    ELSE 'Other'
  END as change_type

FROM updates_to_apply
LEFT JOIN entity_interact_ids eii ON eii.entity_id_int = updates_to_apply.entity_id
LEFT JOIN entity_campaign_ids eci ON eci.entity_id = updates_to_apply.entity_id
ORDER BY campaign_id, entity_id, field_name
