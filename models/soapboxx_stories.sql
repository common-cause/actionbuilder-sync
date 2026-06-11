-- Soapboxx storytelling submissions aggregated by email address.
-- One row per unique normalized email. All-time count (not time-windowed):
-- Soapboxx is a low-volume, high-effort signal (a recorded video / photo /
-- written-story testimonial), so any submission counts — mirroring the NewMode
-- pattern rather than the time-windowed Action Network / Mobilize patterns.
--
-- Source views (soapboxx_cleaned.cln_soapboxx__*) are NOT deduplicated — each
-- user/submission row appears 1x or 3x; always dedup on `uid` first. See the
-- soapboxx_cleaned dataset README.
--
-- soapboxx_stories = total submissions across all types (video + photo + written
-- story), taken from the per-user `total_activity` count. Story dates are derived
-- from the video and photo submission tables; written stories have no per-row
-- table, so an email with only written stories gets a count but NULL dates.
--
-- The sync string for the AB tag is built downstream in correct_participation_values
-- (NewMode pattern), not here. This table will be sparse until Soapboxx collection
-- ramps with the 1 Million Conversations campaign.

WITH deduped_users AS (
  -- Collapse the 1x/3x row duplication to one row per Soapboxx user (uid)
  SELECT
    uid,
    LOWER(TRIM(email)) AS email_normalized,
    first_name,
    last_name,
    phone,
    postal_code,
    total_activity,
    ROW_NUMBER() OVER (
      PARTITION BY uid
      ORDER BY total_activity DESC, utc_created_at DESC
    ) AS rn
  FROM soapboxx_cleaned.cln_soapboxx__users
  WHERE email IS NOT NULL
    AND TRIM(email) != ''
    -- Exclude Soapboxx's own demo/test accounts (vendor domain). Internal CC
    -- staff addresses are intentionally NOT excluded here — staff can be real
    -- participants; revisit if specific staff test submissions need filtering.
    AND LOWER(TRIM(email)) NOT LIKE '%@soapboxx.com'
),

users_one_row AS (
  SELECT uid, email_normalized, first_name, last_name, phone, postal_code, total_activity
  FROM deduped_users
  WHERE rn = 1
),

submissions AS (
  -- Distinct dated submissions (video + photo), deduped on submission uid.
  -- Written stories have no per-row table, so they are absent here (count only).
  SELECT DISTINCT uid, user_id, DATE(utc_created_at) AS story_date
  FROM soapboxx_cleaned.cln_soapboxx__videos
  WHERE utc_created_at IS NOT NULL

  UNION DISTINCT

  SELECT DISTINCT uid, user_id, DATE(utc_created_at) AS story_date
  FROM soapboxx_cleaned.cln_soapboxx__photos
  WHERE utc_created_at IS NOT NULL
),

submission_dates_by_email AS (
  SELECT
    u.email_normalized,
    MIN(s.story_date) AS first_story_date,
    MAX(s.story_date) AS most_recent_story_date
  FROM submissions s
  INNER JOIN users_one_row u
    ON s.user_id = u.uid
  GROUP BY u.email_normalized
)

SELECT
  u.email_normalized,
  -- Best-available contact info per email (email->uid is 1:1 today; MAX picks a
  -- deterministic non-null value if a future email ever spans multiple uids)
  MAX(u.first_name) AS first_name,
  MAX(u.last_name) AS last_name,
  MAX(u.phone) AS phone,
  MAX(u.postal_code) AS zip_code,
  SUM(u.total_activity) AS soapboxx_stories,   -- all submission types, all-time
  d.first_story_date,
  d.most_recent_story_date
FROM users_one_row u
LEFT JOIN submission_dates_by_email d
  ON u.email_normalized = d.email_normalized
GROUP BY u.email_normalized, d.first_story_date, d.most_recent_story_date
HAVING SUM(u.total_activity) > 0
ORDER BY u.email_normalized
