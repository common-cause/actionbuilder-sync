WITH petitions_deduped AS (
  -- One row per (user, petition): signing the same petition multiple times = 1 action.
  SELECT
    user_id,
    MIN(utc_created_at) AS action_date,
    'petition'          AS action_type
  FROM actionnetwork_cleaned.cln_actionnetwork__signatures
  WHERE utc_created_at IS NOT NULL
    AND user_id IS NOT NULL
  GROUP BY user_id, petition_id
),

deliveries_deduped AS (
  -- One row per (user, letter, day): sending the same letter to multiple officials
  -- in one session counts as 1 action. Re-sending on a different day is a new action.
  SELECT
    user_id,
    MIN(utc_created_at) AS action_date,
    'delivery'          AS action_type
  FROM actionnetwork_cleaned.cln_actionnetwork__deliveries
  WHERE utc_created_at IS NOT NULL
    AND user_id IS NOT NULL
  GROUP BY user_id, letter_id, DATE(utc_created_at)
),

action_events AS (
  SELECT * FROM petitions_deduped
  UNION ALL
  SELECT * FROM deliveries_deduped
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
