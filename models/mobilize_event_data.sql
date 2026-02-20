WITH attended_events AS (
  -- Get all events where the user registered and event has occurred
  SELECT 
    LOWER(TRIM(COALESCE(user__email_address, email_at_signup))) as user_email,
    COALESCE(user__email_address, email_at_signup) as email_raw,
    COALESCE(utc_override_start_date, utc_start_date) as event_date,
    event_id,
    event_type_name,
    status,
    attended
  FROM mobilize_cleaned.cln_mobilize__participations
  WHERE status NOT IN ('CANCELLED')
    AND COALESCE(user__email_address, email_at_signup) IS NOT NULL
    AND COALESCE(utc_override_start_date, utc_start_date) IS NOT NULL
    AND DATE(COALESCE(utc_override_start_date, utc_start_date)) <= CURRENT_DATE()
),

user_event_summary AS (
  -- Calculate the three metrics per user
  SELECT 
    user_email,
    
    -- Events Attended Past 6 Months (number_response)
    COUNT(DISTINCT CASE 
      WHEN DATE(event_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
      THEN event_id 
    END) as events_attended_past_6_months,
    
    -- Most Recent Event Attended (date_response) 
    MAX(DATE(event_date)) as most_recent_event_attended,
    
    -- First Event Attended (date_response)
    MIN(DATE(event_date)) as first_event_attended
    
  FROM attended_events
  GROUP BY user_email
)

SELECT 
  user_email,
  events_attended_past_6_months,
  most_recent_event_attended,
  first_event_attended
FROM user_event_summary
WHERE user_email IS NOT NULL
ORDER BY user_email