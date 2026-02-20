WITH all_entity_emails AS (
  -- Get ALL emails for each entity, normalized
  SELECT 
    owner_id as entity_id,
    email as raw_email,
    LOWER(TRIM(email)) as email_normalized
  FROM actionbuilder_cleaned.cln_actionbuilder__emails
  WHERE owner_type = 'Entity'
    AND status IN ('verified', 'user_added')
    AND email IS NOT NULL
),

primary_emails AS (
  -- Get the primary email for each entity (for display purposes)
  SELECT 
    owner_id as entity_id,
    email,
    ROW_NUMBER() OVER (
      PARTITION BY owner_id 
      ORDER BY 
        CASE WHEN status = 'verified' THEN 1 
             WHEN status = 'user_added' THEN 2 
             ELSE 3 END,
        updated_at DESC
    ) as email_rank
  FROM actionbuilder_cleaned.cln_actionbuilder__emails
  WHERE owner_type = 'Entity'
    AND status IN ('verified', 'user_added')
    AND email IS NOT NULL
),

all_entity_phones AS (
  -- Get ALL phone numbers for each entity, normalized to 10 digits
  SELECT 
    owner_id as entity_id,
    number as raw_number,
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(number, r'^\+', ''), r'^1', ''), r'[^\d]', '') as number_normalized
  FROM actionbuilder_cleaned.cln_actionbuilder__phone_numbers
  WHERE owner_type = 'Entity'
    AND status IN ('verified', 'user_added')
    AND number IS NOT NULL
    AND LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(number, r'^\+', ''), r'^1', ''), r'[^\d]', '')) = 10
),

primary_phones AS (
  -- Get the primary phone for each entity (for display purposes)
  SELECT 
    owner_id as entity_id,
    number as raw_number,
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(number, r'^\+', ''), r'^1', ''), r'[^\d]', '') as number_normalized,
    ROW_NUMBER() OVER (
      PARTITION BY owner_id 
      ORDER BY 
        CASE WHEN status = 'verified' THEN 1 
             WHEN status = 'user_added' THEN 2 
             ELSE 3 END,
        updated_at DESC
    ) as phone_rank
  FROM actionbuilder_cleaned.cln_actionbuilder__phone_numbers
  WHERE owner_type = 'Entity'
    AND status IN ('verified', 'user_added')
    AND number IS NOT NULL
    AND LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(number, r'^\+', ''), r'^1', ''), r'[^\d]', '')) = 10
),

scaletowin_calls_by_entity AS (
  -- Sum ScaleToWin calls across all phone numbers for each entity
  SELECT 
    aep.entity_id,
    SUM(scd.phone_bank_calls_made) as total_phone_bank_calls_made
  FROM all_entity_phones aep
  INNER JOIN {{ ref('scaletowin_call_data') }} scd
    ON aep.number_normalized = scd.caller_phone_number
  GROUP BY aep.entity_id
),

actionbuilder_entities_with_contacts AS (
  -- Join entities with their primary contact info
  SELECT 
    e.id as entity_id,
    e.first_name,
    e.last_name,
    CONCAT(COALESCE(e.first_name, ''), ' ', COALESCE(e.last_name, '')) as full_name,
    pe.email as primary_email,
    pp.number_normalized as primary_phone
    
  FROM actionbuilder_cleaned.cln_actionbuilder__entities e
  LEFT JOIN primary_emails pe
    ON e.id = pe.entity_id AND pe.email_rank = 1
  LEFT JOIN primary_phones pp
    ON e.id = pp.entity_id AND pp.phone_rank = 1
),

mobilize_events_by_entity AS (
  -- Aggregate Mobilize event data across all email addresses for each entity
  SELECT 
    aee.entity_id,
    SUM(med.events_attended_past_6_months) as total_events_attended_past_6_months,
    MAX(med.most_recent_event_attended) as most_recent_event_attended,
    MIN(med.first_event_attended) as first_event_attended
  FROM all_entity_emails aee
  INNER JOIN {{ ref('mobilize_event_data') }} med
    ON aee.email_normalized = LOWER(TRIM(med.user_email))
  GROUP BY aee.entity_id
),

action_network_actions_by_entity AS (
  -- Aggregate Action Network actions across all email addresses for each entity
  SELECT 
    aee.entity_id,
    SUM(an6.total_actions_6_months) as total_action_network_actions_6mo
  FROM all_entity_emails aee
  INNER JOIN {{ ref('action_network_6mo_actions') }} an6
    ON aee.email_normalized = an6.email_normalized
  GROUP BY aee.entity_id
),

state_action_network_by_entity AS (
  -- Aggregate State Action Network data across all email addresses for each entity
  SELECT 
    aee.entity_id,
    MAX(satp.state_actions_6_months) as state_actions_6_months,
    MAX(CASE WHEN satp.top_state_action_taker = TRUE THEN 1 ELSE 0 END) as top_state_action_taker
  FROM all_entity_emails aee
  INNER JOIN {{ ref('state_action_network_top_performers') }} satp
    ON aee.email_normalized = satp.email_normalized
  GROUP BY aee.entity_id
),

combined_external_data AS (
  -- Combine ActionBuilder entities with external data via direct matching
  SELECT 
    ab.entity_id,
    ab.first_name,
    ab.last_name,
    ab.full_name,
    ab.primary_email,
    ab.primary_phone,
    
    -- Mobilize event data
    COALESCE(mbe.total_events_attended_past_6_months, 0) as events_attended_past_6_months,
    mbe.most_recent_event_attended,
    mbe.first_event_attended,
    
    -- Action Network online actions data
    COALESCE(anbe.total_action_network_actions_6mo, 0) as action_network_actions_6mo,
    
    -- State Action Network data
    COALESCE(sabe.state_actions_6_months, 0) as state_actions_6_months,
    CASE WHEN sabe.top_state_action_taker = 1 THEN TRUE ELSE FALSE END as top_state_action_taker,
    
    -- ScaleToWin call data
    COALESCE(stw.total_phone_bank_calls_made, 0) as phone_bank_calls_made,
    
    -- Data availability flags
    CASE WHEN mbe.entity_id IS NOT NULL THEN TRUE ELSE FALSE END as has_mobilize_events,
    CASE WHEN anbe.entity_id IS NOT NULL THEN TRUE ELSE FALSE END as has_action_network_actions,
    CASE WHEN sabe.entity_id IS NOT NULL THEN TRUE ELSE FALSE END as has_state_action_network,
    CASE WHEN stw.entity_id IS NOT NULL THEN TRUE ELSE FALSE END as has_scaletowin_calls
    
  FROM actionbuilder_entities_with_contacts ab
  LEFT JOIN mobilize_events_by_entity mbe
    ON ab.entity_id = mbe.entity_id
  LEFT JOIN action_network_actions_by_entity anbe
    ON ab.entity_id = anbe.entity_id
  LEFT JOIN state_action_network_by_entity sabe
    ON ab.entity_id = sabe.entity_id
  LEFT JOIN scaletowin_calls_by_entity stw
    ON ab.entity_id = stw.entity_id
),

formatted_values AS (
  -- Format the values according to sync field requirements
  SELECT 
    entity_id,
    first_name,
    last_name,
    full_name,
    primary_email,
    primary_phone,
    
    -- Raw values
    events_attended_past_6_months,
    most_recent_event_attended,
    first_event_attended,
    action_network_actions_6mo,
    state_actions_6_months,
    top_state_action_taker,
    phone_bank_calls_made,
    
    -- Formatted values for sync
    CAST(events_attended_past_6_months AS STRING) as events_attended_past_6_months_value,
    
    CASE 
      WHEN most_recent_event_attended IS NOT NULL 
      THEN FORMAT_DATE('%Y-%m-%d', most_recent_event_attended)
      ELSE NULL 
    END as most_recent_event_attended_value,
    
    CASE 
      WHEN first_event_attended IS NOT NULL 
      THEN FORMAT_DATE('%Y-%m-%d', first_event_attended)
      ELSE NULL 
    END as first_event_attended_value,
    
    CAST(action_network_actions_6mo AS STRING) as action_network_actions_6mo_value,
    
    CAST(state_actions_6_months AS STRING) as state_actions_6mo_value,
    
    -- Top state action taker as standard tag (only for top performers, NULL otherwise)
    CASE WHEN top_state_action_taker = TRUE THEN 'Top State Action Taker' ELSE NULL END as top_state_action_taker_value,
    
    CAST(phone_bank_calls_made AS STRING) as phone_bank_calls_made_value,
    
    -- Data source tracking
    has_mobilize_events,
    has_action_network_actions,
    has_state_action_network,
    has_scaletowin_calls,
    
    -- Overall data completeness
    CASE 
      WHEN events_attended_past_6_months > 0 OR most_recent_event_attended IS NOT NULL 
           OR first_event_attended IS NOT NULL OR action_network_actions_6mo > 0 
           OR state_actions_6_months > 0 OR phone_bank_calls_made > 0 
      THEN TRUE 
      ELSE FALSE 
    END as has_participation_data
    
  FROM combined_external_data
)

-- Final output with sync field identifiers
SELECT 
  entity_id,
  first_name,
  last_name,
  full_name,
  primary_email,
  primary_phone,
  
  -- Participation metrics
  events_attended_past_6_months,
  most_recent_event_attended,
  first_event_attended,
  action_network_actions_6mo,
  state_actions_6_months,
  top_state_action_taker,
  phone_bank_calls_made,
  
  -- Formatted values for sync
  events_attended_past_6_months_value,
  most_recent_event_attended_value,
  first_event_attended_value,
  action_network_actions_6mo_value,
  state_actions_6mo_value,
  top_state_action_taker_value,
  phone_bank_calls_made_value,
  
  -- Sync field mappings with embedded values and response types
  CONCAT('Participation:|:Event Attendance Summary:|:Events Attended Past 6 Months:|:number_response:', events_attended_past_6_months_value) as events_6mo_sync_string,
  CONCAT('Participation:|:Event Attendance History:|:Most Recent Event Attended:|:date_response:', COALESCE(most_recent_event_attended_value, '')) as recent_event_sync_string,
  CONCAT('Participation:|:Event Attendance History:|:First Event Attended:|:date_response:', COALESCE(first_event_attended_value, '')) as first_event_sync_string,
  CONCAT('Participation:|:Online Actions Past 6 Months:|:Action Network Actions:|:number_response:', action_network_actions_6mo_value) as action_network_6mo_sync_string,
  CONCAT('Participation:|:Online Actions Past 6 Months:|:Action Network State Actions:|:number_response:', state_actions_6mo_value) as state_actions_6mo_sync_string,
  CASE 
    WHEN top_state_action_taker_value IS NOT NULL 
    THEN CONCAT('Participation:|:State Online Actions:|:Top State Action Taker:|:standard_response:', top_state_action_taker_value)
    ELSE NULL
  END as top_state_action_taker_sync_string,
  CONCAT('Participation:|:Event Attendance Summary:|:Phone Bank Calls Made:|:number_response:', phone_bank_calls_made_value) as phone_calls_sync_string,
  
  -- Data quality indicators
  has_mobilize_events,
  has_action_network_actions,
  has_state_action_network,
  has_scaletowin_calls,
  has_participation_data,
  
  -- Summary metrics for analysis
  CASE 
    WHEN events_attended_past_6_months > 0 AND phone_bank_calls_made > 0 AND action_network_actions_6mo > 0 THEN 'Very High Activity'
    WHEN (events_attended_past_6_months > 0 AND phone_bank_calls_made > 0) OR 
         (events_attended_past_6_months > 0 AND action_network_actions_6mo > 0) OR
         (phone_bank_calls_made > 0 AND action_network_actions_6mo > 0) THEN 'High Activity'
    WHEN events_attended_past_6_months > 0 OR phone_bank_calls_made > 0 OR action_network_actions_6mo > 0 THEN 'Some Activity'
    ELSE 'No Recent Activity'
  END as activity_level

FROM formatted_values
ORDER BY entity_id