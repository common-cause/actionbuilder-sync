-- NewMode submissions aggregated by email address.
-- One row per unique normalized email. All-time count (not 6-month filtered).
-- This table will be empty until NewMode data starts flowing.
SELECT
  LOWER(TRIM(contact_email)) as email_normalized,
  COUNT(DISTINCT submission_id) as newmode_submission_count

FROM newmode_cleaned.cln_newmode__submissions

WHERE contact_email IS NOT NULL
  AND testmode IS DISTINCT FROM TRUE  -- exclude test/dev submissions

GROUP BY LOWER(TRIM(contact_email))
