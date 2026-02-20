WITH connected_calls_with_caller AS (
  -- Get all connected calls with caller phone numbers
  SELECT 
    c.caller_id,
    c.created_at as call_date,
    c.duration,
    c.status,
    c.connected_at,
    caller.phone_number as caller_phone_raw
  FROM scaletowin_dialer_cleaned.cln_scaletowin_dialer__calls c
  INNER JOIN scaletowin_dialer_cleaned.cln_scaletowin_dialer__callers caller
    ON c.caller_id = caller.id
  WHERE c.connected_at IS NOT NULL
    AND caller.phone_number IS NOT NULL
),

caller_call_summary AS (
  -- Count phone bank calls made per caller phone number
  SELECT 
    REGEXP_REPLACE(caller_phone_raw, r'^1', '') as caller_phone_number,
    
    -- Phone Bank Calls Made (number_response)
    COUNT(*) as phone_bank_calls_made
    
  FROM connected_calls_with_caller
  GROUP BY REGEXP_REPLACE(caller_phone_raw, r'^1', '')
)

SELECT 
  caller_phone_number,
  phone_bank_calls_made
FROM caller_call_summary
WHERE caller_phone_number IS NOT NULL
ORDER BY caller_phone_number