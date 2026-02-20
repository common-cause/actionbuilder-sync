WITH action_network_data AS (
  -- Get Action Network online actions data for past 6 months
  SELECT 
    LOWER(TRIM(email)) as email_normalized,
    user_id,
    
    -- Total actions in past 6 months (petitions + deliveries)
    CAST(actions_6_months AS INT64) as total_actions_6_months,
    
    -- Breakdown by action type
    CAST(petitions_6_months AS INT64) as petitions_6_months,
    CAST(deliveries_6_months AS INT64) as deliveries_6_months,
    
    -- Additional metrics for context
    CAST(actions_12_months AS INT64) as total_actions_12_months,
    CAST(actions_all_time AS INT64) as total_actions_all_time,
    
    -- Date fields (already TIMESTAMP, no parsing needed)
    first_action_date,
    latest_action_date,
    user_created_at,
    user_updated_at
    
  FROM actionbuilder_sync.action_network_actions
  WHERE email IS NOT NULL
    AND actions_6_months IS NOT NULL
)

SELECT 
  email_normalized,
  user_id,
  total_actions_6_months,
  petitions_6_months,
  deliveries_6_months,
  
  -- Formatted value for sync string
  CAST(total_actions_6_months AS STRING) as action_network_actions_6mo_value,
  
  -- Create sync string for the new field
  CONCAT('Participation:|:Online Actions Past 6 Months:|:Action Network Actions:|:number_response:', CAST(total_actions_6_months AS STRING)) as action_network_6mo_sync_string,
  
  -- Additional context fields
  total_actions_12_months,
  total_actions_all_time,
  first_action_date,
  latest_action_date,
  
  -- Activity level classification
  CASE 
    WHEN total_actions_6_months >= 100 THEN 'Very High'
    WHEN total_actions_6_months >= 50 THEN 'High'
    WHEN total_actions_6_months >= 10 THEN 'Moderate'
    WHEN total_actions_6_months >= 1 THEN 'Low'
    ELSE 'None'
  END as activity_level_6mo
  
FROM action_network_data
WHERE total_actions_6_months >= 0  -- Include users with 0 actions for completeness
ORDER BY email_normalized