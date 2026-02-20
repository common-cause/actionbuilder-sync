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
    phone_calls_sync_string
  FROM {{ ref('correct_participation_values') }}
  WHERE has_participation_data = TRUE  -- Only entities with actual participation data
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
    'Phone Bank Calls Made'
  )
),

pivot_current_values AS (
  -- Pivot current ActionBuilder values to one row per entity/campaign,
  -- including removal strings for each field
  SELECT
    entity_id,
    campaign_id,
    MAX(CASE WHEN tag_name = 'Events Attended Past 6 Months' THEN current_value END) as current_events_6mo,
    MAX(CASE WHEN tag_name = 'Most Recent Event Attended'    THEN current_value END) as current_recent_event,
    MAX(CASE WHEN tag_name = 'First Event Attended'          THEN current_value END) as current_first_event,
    MAX(CASE WHEN tag_name = 'Action Network Actions'        THEN current_value END) as current_action_network,
    MAX(CASE WHEN tag_name = 'Action Network State Actions'  THEN current_value END) as current_state_actions,
    MAX(CASE WHEN tag_name = 'Top State Action Taker'        THEN current_value END) as current_top_state_performer,
    MAX(CASE WHEN tag_name = 'Phone Bank Calls Made'         THEN current_value END) as current_phone_calls,

    -- Removal strings: tag-interact-id:|:tagging-interact-id
    MAX(CASE WHEN tag_name = 'Events Attended Past 6 Months' THEN removal_string END) as removal_ids_events_6mo,
    MAX(CASE WHEN tag_name = 'Most Recent Event Attended'    THEN removal_string END) as removal_ids_recent_event,
    MAX(CASE WHEN tag_name = 'First Event Attended'          THEN removal_string END) as removal_ids_first_event,
    MAX(CASE WHEN tag_name = 'Action Network Actions'        THEN removal_string END) as removal_ids_action_network,
    MAX(CASE WHEN tag_name = 'Action Network State Actions'  THEN removal_string END) as removal_ids_state_actions,
    MAX(CASE WHEN tag_name = 'Top State Action Taker'        THEN removal_string END) as removal_ids_top_state_performer,
    MAX(CASE WHEN tag_name = 'Phone Bank Calls Made'         THEN removal_string END) as removal_ids_phone_calls
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

    -- Correct values from external data
    cv.events_attended_past_6_months_value          as correct_events_6mo,
    COALESCE(cv.most_recent_event_attended_value, '') as correct_recent_event,
    COALESCE(cv.first_event_attended_value, '')     as correct_first_event,
    cv.action_network_actions_6mo_value             as correct_action_network,
    cv.state_actions_6mo_value                      as correct_state_actions,
    COALESCE(cv.top_state_action_taker_value, '')   as correct_top_state_performer,
    cv.phone_bank_calls_made_value                  as correct_phone_calls,

    -- Sync strings for updates
    cv.events_6mo_sync_string,
    cv.recent_event_sync_string,
    cv.first_event_sync_string,
    cv.action_network_6mo_sync_string,
    cv.state_actions_6mo_sync_string,
    cv.top_state_action_taker_sync_string,
    cv.phone_calls_sync_string,

    -- Removal strings (NULL when there is no existing value to remove)
    pcv.removal_ids_events_6mo,
    pcv.removal_ids_recent_event,
    pcv.removal_ids_first_event,
    pcv.removal_ids_action_network,
    pcv.removal_ids_state_actions,
    pcv.removal_ids_top_state_performer,
    pcv.removal_ids_phone_calls,

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
    END as phone_calls_needs_update

  FROM correct_values cv
  INNER JOIN entities_in_active_campaigns eac
    ON cv.entity_id = eac.entity_id
  LEFT JOIN pivot_current_values pcv
    ON cv.entity_id = pcv.entity_id
    AND eac.campaign_id = pcv.campaign_id
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
)

-- Final output: only records that need updates with separate columns per field group
SELECT
  campaign_id,
  -- Use interact_id from entities table instead of integer id
  (SELECT e.interact_id
   FROM actionbuilder_cleaned.cln_actionbuilder__entities e
   WHERE e.id = updates_to_apply.entity_id) as entity_id,
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

  current_value,
  correct_value,

  -- Add comma-separated list of campaign interact_ids for this entity
  (SELECT STRING_AGG(c.interact_id, ',')
   FROM actionbuilder_cleaned.cln_actionbuilder__campaigns c
   INNER JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns_entities ce
     ON c.id = ce.campaign_id
   WHERE ce.entity_id = updates_to_apply.entity_id
     AND c.status = 'active'
  ) as campaign_ids,

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
ORDER BY campaign_id, entity_id, field_name
