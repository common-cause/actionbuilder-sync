SELECT
  first_name,
  last_name,
  phone_number,
  email,
  COALESCE(s.abbreviation, mlq.state) AS state,  -- Resolve to postal code
  county,
  zip_code,
  source_code,
  min(created_at) created_at,
  shifted_2024,
  events_6m,
  phone_bank_dials,
  max(action_network_actions) action_network_actions,
  
  -- Add field name columns for upload interface
  action_network_field,
  events_field,
  pb_field,
  first_event_field,
  mr_event_field,
  
  -- Add event dates
  first_event_date,
  mr_event_date

FROM actionbuilder_sync.master_load_qualifiers mlq
LEFT JOIN actionnetwork_views.states s ON (
  mlq.state = s.name OR mlq.state = s.abbreviation
)
WHERE (email IS NOT NULL OR phone_number IS NOT NULL)
  AND s.abbreviation IS NOT NULL  -- Only include records that match US states

GROUP BY   
  first_name,
  last_name,
  phone_number,
  email,
  COALESCE(s.abbreviation, mlq.state),  -- Group by resolved state
  county,
  zip_code,
  source_code,
  shifted_2024,
  events_6m,
  phone_bank_dials,
  
  -- Add field name columns for upload interface
  action_network_field,
  events_field,
  pb_field,
  first_event_field,
  mr_event_field,
  
  -- Add event dates
  first_event_date,
  mr_event_date
  
ORDER BY 
  CASE WHEN shifted_2024 = 'Y' THEN 1 ELSE 2 END,
  last_name, first_name