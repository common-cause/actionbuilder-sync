-- Filtered view of updates_needed for the Test campaign only.
-- Use this to inspect, verify, and spot-check what will happen when
-- sync.py update_records runs against the Test campaign (0e41ca37...).
--
-- One row per (entity, field) needing an update. Includes current and
-- correct values, the sync/removal strings that will be posted, and
-- the entity's primary email for easy human identification.
--
-- Run first with: bash dbt.sh run -s test_campaign_updates

SELECT
  un.*,
  e.first_name,
  e.last_name,
  el.email AS primary_email
FROM {{ ref('updates_needed') }} un
JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns c
  ON un.campaign_id = c.id
JOIN actionbuilder_cleaned.cln_actionbuilder__entities e
  ON un.entity_id = e.interact_id
LEFT JOIN {{ ref('entity_lookup_debug') }} el
  ON un.entity_id = el.interact_id
WHERE c.interact_id = '0e41ca37-e05d-499c-943b-9d08dc8725b0'
ORDER BY un.field_name, un.change_type, e.last_name, e.first_name
