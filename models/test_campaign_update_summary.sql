-- Summary of pending updates for the Test campaign, grouped by field and change type.
-- Use this as a quick sanity-check dashboard before running the live sync.
--
-- change_type values:
--   New Value   -- entity has no current tag; will write for the first time
--   Update Value -- entity has a tag that differs from correct value; will replace
--   Clear Value  -- entity has a tag but correct value is 0/''; will remove without replacing
--
-- Run first with: bash dbt.sh run -s test_campaign_update_summary

SELECT
  un.field_name,
  un.change_type,
  COUNT(DISTINCT un.entity_id)                       AS entity_count,
  MIN(CAST(un.current_value AS FLOAT64))             AS current_min,
  MAX(CAST(un.current_value AS FLOAT64))             AS current_max,
  MIN(CAST(un.correct_value AS FLOAT64))             AS correct_min,
  MAX(CAST(un.correct_value AS FLOAT64))             AS correct_max,
  AVG(
    ABS(
      CAST(NULLIF(un.correct_value, '') AS FLOAT64) -
      CAST(NULLIF(un.current_value, '') AS FLOAT64)
    )
  )                                                   AS avg_abs_delta
FROM {{ ref('updates_needed') }} un
JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns c
  ON un.campaign_id = c.id
WHERE c.interact_id = '0e41ca37-e05d-499c-943b-9d08dc8725b0'
  AND un.field_name NOT IN ('Most Recent Event Attended', 'First Event Attended')
  -- date fields excluded from numeric aggregates above; add separate block below
GROUP BY un.field_name, un.change_type

UNION ALL

-- Date fields: count only (no numeric aggregates)
SELECT
  un.field_name,
  un.change_type,
  COUNT(DISTINCT un.entity_id) AS entity_count,
  NULL                          AS current_min,
  NULL                          AS current_max,
  NULL                          AS correct_min,
  NULL                          AS correct_max,
  NULL                          AS avg_abs_delta
FROM {{ ref('updates_needed') }} un
JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns c
  ON un.campaign_id = c.id
WHERE c.interact_id = '0e41ca37-e05d-499c-943b-9d08dc8725b0'
  AND un.field_name IN ('Most Recent Event Attended', 'First Event Attended')
GROUP BY un.field_name, un.change_type

ORDER BY field_name, change_type
