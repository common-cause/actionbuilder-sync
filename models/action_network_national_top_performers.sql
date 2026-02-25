-- Top 50 Action Network activists nationally, ranked by 6-month action count.
-- Used to populate the "Top National Action Network Activist" standard tag in AB.
WITH ranked_nationally AS (
  SELECT
    email_normalized,
    user_id,
    total_actions_6_months,
    ROW_NUMBER() OVER (
      ORDER BY total_actions_6_months DESC, email_normalized
    ) as national_rank
  FROM {{ ref('action_network_6mo_actions') }}
  WHERE total_actions_6_months > 0
)

SELECT
  email_normalized,
  user_id,
  total_actions_6_months,
  national_rank,
  TRUE as top_national_action_taker,
  'Top National Action Network Activist' as top_national_value,
  'Participation:|:National Online Actions:|:Top National Action Network Activist:|:standard_response:Top National Action Network Activist' as top_national_sync_string

FROM ranked_nationally
WHERE national_rank <= 50
ORDER BY national_rank
