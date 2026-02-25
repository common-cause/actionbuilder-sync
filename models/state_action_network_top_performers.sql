WITH state_groups AS (
  -- Identify state groups by parsing names from children of national group 64151
  SELECT
    g.id as group_id,
    g.name as group_name,
    g.parent_id,
    -- Extract state name from group name patterns
    CASE
      -- Pattern: "Common Cause [State]"
      WHEN REGEXP_CONTAINS(g.name, r'^Common Cause (.+)$')
        THEN REGEXP_EXTRACT(g.name, r'^Common Cause (.+)$')
      -- Pattern: "[State] Common Cause"
      WHEN REGEXP_CONTAINS(g.name, r'^(.+) Common Cause$')
        THEN REGEXP_EXTRACT(g.name, r'^(.+) Common Cause$')
      ELSE NULL
    END as extracted_state,

    -- Validate against known state names
    CASE
      WHEN REGEXP_CONTAINS(g.name, r'(?i)(Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming|District of Columbia|Washington DC|DC)')
        THEN REGEXP_EXTRACT(g.name, r'(?i)(Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming|District of Columbia|Washington DC|DC)')
      ELSE NULL
    END as state_name
  FROM actionnetwork_cleaned.cln_actionnetwork__groups g
  WHERE g.parent_id = 64151  -- Children of national organization
    AND g.status = 2  -- Active groups (status 2 in Action Network)
),

validated_state_groups AS (
  -- Only keep groups where we successfully extracted a state name
  SELECT
    group_id,
    group_name,
    parent_id,
    CASE
      WHEN state_name IN ('Washington DC', 'DC') THEN 'District of Columbia'
      ELSE state_name
    END as state_name
  FROM state_groups
  WHERE state_name IS NOT NULL
),

user_state_residence AS (
  -- Get user state of residence from Action Network core fields
  SELECT
    u.id as user_id,
    u.email,
    LOWER(TRIM(u.email)) as email_normalized,
    cf.state as residence_state
  FROM actionnetwork_cleaned.cln_actionnetwork__users u
  INNER JOIN actionnetwork_cleaned.cln_actionnetwork__core_fields cf
    ON u.id = cf.user_id
  WHERE cf.state IS NOT NULL
    AND u.email IS NOT NULL
),

state_signatures AS (
  -- Get signature actions from state groups in past 6 months
  SELECT
    usr.user_id,
    usr.email_normalized,
    usr.residence_state,
    vsg.state_name as action_state,
    s.utc_created_at as action_date,
    'signature' as action_type,
    s.petition_id as action_id
  FROM actionnetwork_cleaned.cln_actionnetwork__signatures s
  INNER JOIN actionnetwork_cleaned.cln_actionnetwork__petitions p
    ON s.petition_id = p.id
  INNER JOIN validated_state_groups vsg
    ON p.group_id = vsg.group_id
  INNER JOIN user_state_residence usr
    ON s.user_id = usr.user_id
  WHERE DATETIME(s.utc_created_at) >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 6 MONTH)
    -- Only count actions in user's state of residence
    AND UPPER(usr.residence_state) = UPPER(vsg.state_name)
),

state_deliveries AS (
  -- Get delivery actions from state groups in past 6 months
  SELECT
    usr.user_id,
    usr.email_normalized,
    usr.residence_state,
    vsg.state_name as action_state,
    d.utc_created_at as action_date,
    'delivery' as action_type,
    d.letter_id as action_id
  FROM actionnetwork_cleaned.cln_actionnetwork__deliveries d
  INNER JOIN actionnetwork_cleaned.cln_actionnetwork__letters l
    ON d.letter_id = l.id
  INNER JOIN validated_state_groups vsg
    ON l.group_id = vsg.group_id
  INNER JOIN user_state_residence usr
    ON d.user_id = usr.user_id
  WHERE DATETIME(d.utc_created_at) >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 6 MONTH)
    -- Only count actions in user's state of residence
    AND UPPER(usr.residence_state) = UPPER(vsg.state_name)
),

all_state_actions AS (
  -- Combine signatures and deliveries
  SELECT * FROM state_signatures
  UNION ALL
  SELECT * FROM state_deliveries
),

user_state_action_counts AS (
  -- Count distinct state actions per user (one count per unique action_type + action_id combination)
  SELECT
    user_id,
    email_normalized,
    residence_state,
    COUNT(DISTINCT CONCAT(action_type, '-', CAST(action_id AS STRING))) as state_actions_6_months,
    COUNT(DISTINCT CASE WHEN action_type = 'signature' THEN action_id END) as state_petitions_6_months,
    COUNT(DISTINCT CASE WHEN action_type = 'delivery' THEN action_id END) as state_deliveries_6_months,
    MIN(DATE(action_date)) as first_state_action_date,
    MAX(DATE(action_date)) as most_recent_state_action_date
  FROM all_state_actions
  GROUP BY user_id, email_normalized, residence_state
  HAVING COUNT(DISTINCT CONCAT(action_type, '-', CAST(action_id AS STRING))) >= 3  -- Minimum 3 distinct state actions required
),

ranked_users AS (
  -- Rank users by state action count within each state (top 50 per state)
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY residence_state
      ORDER BY state_actions_6_months DESC, user_id
    ) as action_rank
  FROM user_state_action_counts
),

final_results AS (
  -- Apply per-state top 50 threshold
  SELECT
    ru.user_id,
    ru.email_normalized,
    ru.residence_state,
    ru.state_actions_6_months,
    ru.state_petitions_6_months,
    ru.state_deliveries_6_months,
    ru.first_state_action_date,
    ru.most_recent_state_action_date,
    ru.action_rank,

    -- Top performer flag: top 50 within their state
    CASE
      WHEN ru.action_rank <= 50 THEN TRUE
      ELSE FALSE
    END as top_state_action_taker,

    -- Sync strings for ActionBuilder integration
    CAST(ru.state_actions_6_months AS STRING) as state_actions_6mo_value,

    CONCAT('Participation:|:Online Actions Past 6 Months:|:Action Network State Actions:|:number_response:', CAST(ru.state_actions_6_months AS STRING)) as state_actions_sync_string,

    -- Top performer as standard tag (not boolean)
    CASE
      WHEN ru.action_rank <= 50
      THEN 'Participation:|:State Online Actions:|:Top State Action Taker:|:standard_response:Top State Action Taker'
      ELSE NULL
    END as top_performer_sync_string

  FROM ranked_users ru
)

-- Final output
SELECT
  user_id,
  email_normalized,
  residence_state,
  state_actions_6_months,
  state_petitions_6_months,
  state_deliveries_6_months,
  first_state_action_date,
  most_recent_state_action_date,
  action_rank,
  top_state_action_taker,
  state_actions_6mo_value,
  state_actions_sync_string,
  top_performer_sync_string

FROM final_results
ORDER BY residence_state, action_rank
