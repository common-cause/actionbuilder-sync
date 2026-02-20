WITH action_events AS (
  -- Get petition signatures
  SELECT 
    s.user_id,
    s.utc_created_at as action_date,
    'petition' as action_type,
    s.petition_id as action_id
  FROM actionnetwork_cleaned.cln_actionnetwork__signatures s
  WHERE s.utc_created_at IS NOT NULL
    AND s.user_id IS NOT NULL
  
  UNION ALL
  
  -- Get message deliveries
  SELECT 
    d.user_id,
    d.utc_created_at as action_date,
    'delivery' as action_type,
    d.letter_id as action_id
  FROM actionnetwork_cleaned.cln_actionnetwork__deliveries d
  WHERE d.utc_created_at IS NOT NULL
    AND d.user_id IS NOT NULL
),
user_action_summary AS (
  SELECT 
    ae.user_id,
    
    -- Actions in past 6 months
    COUNT(CASE 
      WHEN DATE(ae.action_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
      THEN 1 
    END) as actions_6m,
    
    -- Actions in past year
    COUNT(CASE 
      WHEN DATE(ae.action_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
      THEN 1 
    END) as actions_12m,
    
    -- All time actions
    COUNT(*) as actions_all_time,
    
    -- Action type breakdowns for past 6 months
    COUNT(CASE 
      WHEN DATE(ae.action_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
        AND ae.action_type = 'petition'
      THEN 1 
    END) as petitions_6m,
    
    COUNT(CASE 
      WHEN DATE(ae.action_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
        AND ae.action_type = 'delivery'
      THEN 1 
    END) as deliveries_6m,
    
    -- Action type breakdowns for past year
    COUNT(CASE 
      WHEN DATE(ae.action_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
        AND ae.action_type = 'petition'
      THEN 1 
    END) as petitions_12m,
    
    COUNT(CASE 
      WHEN DATE(ae.action_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
        AND ae.action_type = 'delivery'
      THEN 1 
    END) as deliveries_12m,
    
    -- All time action type breakdowns
    COUNT(CASE WHEN ae.action_type = 'petition' THEN 1 END) as petitions_all_time,
    COUNT(CASE WHEN ae.action_type = 'delivery' THEN 1 END) as deliveries_all_time,
    
    -- Date ranges for reference
    MIN(ae.action_date) as first_action_date,
    MAX(ae.action_date) as latest_action_date
    
  FROM action_events ae
  GROUP BY ae.user_id
)
-- Final output with user email addresses
SELECT 
  u.email,
  u.id as user_id,
  
  -- Core action counts
  COALESCE(uas.actions_6m, 0) as actions_6_months,
  COALESCE(uas.actions_12m, 0) as actions_12_months, 
  COALESCE(uas.actions_all_time, 0) as actions_all_time,
  
  -- Petition-specific counts
  COALESCE(uas.petitions_6m, 0) as petitions_6_months,
  COALESCE(uas.petitions_12m, 0) as petitions_12_months,
  COALESCE(uas.petitions_all_time, 0) as petitions_all_time,
  
  -- Message delivery counts
  COALESCE(uas.deliveries_6m, 0) as deliveries_6_months,
  COALESCE(uas.deliveries_12m, 0) as deliveries_12_months,
  COALESCE(uas.deliveries_all_time, 0) as deliveries_all_time,
  
  -- Activity date ranges
  uas.first_action_date,
  uas.latest_action_date,
  
  -- User metadata for reference
  u.utc_created_at as user_created_at,
  u.utc_updated_at as user_updated_at
FROM actionnetwork_cleaned.cln_actionnetwork__users u
LEFT JOIN user_action_summary uas ON u.id = uas.user_id
WHERE u.email IS NOT NULL
  AND u.email != ''
ORDER BY uas.actions_all_time DESC, u.email