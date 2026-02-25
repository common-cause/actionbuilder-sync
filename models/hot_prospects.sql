-- Top 10 highest-activity AB entities per campaign who haven't been called recently
-- and haven't been assessed at level 2+. These are the best people to reach out to.
--
-- Criteria (all must apply):
--   - Entity is in an active non-Test AB campaign
--   - NOT called in the past 30 days (no contact_attempt within 30 days)
--   - Current assessment level < 2 (or no assessment)
--   - Has any activity in the past 6 months (Mobilize, AN, NewMode) OR any STW calls
--
-- Ranked by total activity volume descending; top 10 per campaign.
-- When an entity falls off the list, updates_needed removes the Hot Prospect tag.

WITH all_entity_emails AS (
  SELECT
    owner_id as entity_id,
    LOWER(TRIM(email)) as email_normalized
  FROM actionbuilder_cleaned.cln_actionbuilder__emails
  WHERE owner_type = 'Entity'
    AND status IN ('verified', 'user_added')
    AND email IS NOT NULL
),

all_entity_phones AS (
  SELECT
    owner_id as entity_id,
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(number, r'^\+', ''), r'^1', ''), r'[^\d]', '') as number_normalized
  FROM actionbuilder_cleaned.cln_actionbuilder__phone_numbers
  WHERE owner_type = 'Entity'
    AND status IN ('verified', 'user_added')
    AND number IS NOT NULL
    AND LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(number, r'^\+', ''), r'^1', ''), r'[^\d]', '')) = 10
),

ab_entities_in_active_campaigns AS (
  SELECT DISTINCT
    e.id as entity_id,
    c.id as campaign_id_int,
    c.interact_id as campaign_interact_id
  FROM actionbuilder_cleaned.cln_actionbuilder__entities e
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns_entities ce
    ON e.id = ce.entity_id
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns c
    ON ce.campaign_id = c.id
  WHERE c.status = 'active'
    AND c.name != 'Test'
),

recently_called AS (
  -- Entities with any contact attempt in the past 30 days
  SELECT DISTINCT entity_id
  FROM actionbuilder_cleaned.cln_actionbuilder__contact_attempts
  WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
),

latest_assessments AS (
  SELECT
    owner_id as entity_id,
    level,
    ROW_NUMBER() OVER (PARTITION BY owner_id ORDER BY utc_created_at DESC) as rn
  FROM actionbuilder_cleaned.cln_actionbuilder__assessments
  WHERE owner_type = 'Entity'
),

current_assessments AS (
  SELECT entity_id, level
  FROM latest_assessments
  WHERE rn = 1
),

-- Activity in past 6 months per entity
mobilize_activity AS (
  SELECT
    aee.entity_id,
    COUNT(DISTINCT mp.event_id) as mobilize_events_6mo
  FROM all_entity_emails aee
  INNER JOIN mobilize_cleaned.cln_mobilize__participations mp
    ON aee.email_normalized = LOWER(TRIM(mp.user__email_address))
  WHERE mp.attended = True
    AND (mp.utc_start_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 183 DAY)
         OR mp.utc_override_start_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 183 DAY))
  GROUP BY aee.entity_id
),

an_activity AS (
  SELECT
    aee.entity_id,
    SUM(an6.total_actions_6_months) as an_actions_6mo
  FROM all_entity_emails aee
  INNER JOIN {{ ref('action_network_6mo_actions') }} an6
    ON aee.email_normalized = an6.email_normalized
  GROUP BY aee.entity_id
),

stw_activity AS (
  -- ScaleToWin totals (no 6-month filter available; any calls = qualifies)
  SELECT
    aep.entity_id,
    SUM(scd.phone_bank_calls_made) as stw_calls
  FROM all_entity_phones aep
  INNER JOIN {{ ref('scaletowin_call_data') }} scd
    ON aep.number_normalized = scd.caller_phone_number
  GROUP BY aep.entity_id
),

newmode_activity AS (
  SELECT
    aee.entity_id,
    COUNT(DISTINCT nm.submission_id) as newmode_count
  FROM all_entity_emails aee
  INNER JOIN newmode_cleaned.cln_newmode__submissions nm
    ON aee.email_normalized = LOWER(TRIM(nm.contact_email))
  WHERE nm.testmode IS DISTINCT FROM TRUE
    AND nm.utc_created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 183 DAY)
  GROUP BY aee.entity_id
),

entity_activity AS (
  SELECT
    ec.entity_id,
    ec.campaign_id_int,
    ec.campaign_interact_id,
    COALESCE(mob.mobilize_events_6mo, 0) as mobilize_events_6mo,
    COALESCE(an.an_actions_6mo, 0) as an_actions_6mo,
    COALESCE(stw.stw_calls, 0) as stw_calls,
    COALESCE(nm.newmode_count, 0) as newmode_count,
    COALESCE(mob.mobilize_events_6mo, 0)
      + COALESCE(an.an_actions_6mo, 0)
      + COALESCE(stw.stw_calls, 0)
      + COALESCE(nm.newmode_count, 0) as total_activity_score
  FROM ab_entities_in_active_campaigns ec
  LEFT JOIN mobilize_activity mob ON ec.entity_id = mob.entity_id
  LEFT JOIN an_activity an ON ec.entity_id = an.entity_id
  LEFT JOIN stw_activity stw ON ec.entity_id = stw.entity_id
  LEFT JOIN newmode_activity nm ON ec.entity_id = nm.entity_id
),

qualified_entities AS (
  -- Apply all three eligibility filters
  SELECT ea.*
  FROM entity_activity ea
  LEFT JOIN recently_called rc ON ea.entity_id = rc.entity_id
  LEFT JOIN current_assessments ca ON ea.entity_id = ca.entity_id
  WHERE ea.total_activity_score > 0       -- must have some measurable activity
    AND rc.entity_id IS NULL              -- not called in the past 30 days
    AND (ca.entity_id IS NULL OR ca.level < 2)  -- no assessment or level 0/1
),

ranked_prospects AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY campaign_id_int
      ORDER BY total_activity_score DESC, entity_id
    ) as prospect_rank
  FROM qualified_entities
)

SELECT
  entity_id,
  campaign_id_int,
  campaign_interact_id,
  mobilize_events_6mo,
  an_actions_6mo,
  stw_calls,
  newmode_count,
  total_activity_score,
  prospect_rank,
  'Hot Prospect' as hot_prospect_value,
  'Engagement:|:Prospect Identification:|:Hot Prospect:|:standard_response:Hot Prospect' as hot_prospect_sync_string

FROM ranked_prospects
WHERE prospect_rank <= 10
ORDER BY campaign_interact_id, prospect_rank
