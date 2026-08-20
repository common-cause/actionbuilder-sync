-- march_on_washington_targets.sql
-- Ranked call list for the March on Washington: the best callable Common Cause
-- supporters living within driving distance of the National Mall, spread across
-- DC / MD / VA, for the DMV organizer (Carlos Childs) to recruit by phone.
--
-- This model is the RANKING LOGIC. The list that was actually pushed to AB is the
-- frozen snapshot table `actionbuilder_sync.march_on_washington_list` (built once
-- from this view — see docs/march_on_washington_list.md). Freezing matters: this
-- view re-ranks every time it is queried (recency of last event moves nightly), so
-- reading it directly would drift the membership out from under the AB writes.
--
-- Pool (all must hold):
--   - AB entity whose most recent address geocodes within RADIUS_MI of the Mall
--   - address state in DC / MD / VA
--   - callable: >=1 phone with status verified/user_added that normalizes to 10 digits
--   - no phone marked do_not_contact or bad (whole entity excluded, not just that line)
--   - member of at least one active non-Test campaign (Test holds a legacy duplicate load)
--   - not on the suppression list, and not one of the organizers themselves
--
-- Ranking — "proven show-ups first". For a physical march, having actually turned
-- out to something before is the strongest available predictor, so event history
-- outranks online volume rather than being summed with it:
--   tier 1  attended a Mobilize event in the last 365 days
--   tier 2  attended a Mobilize event ever
--   tier 3  no attendance on record — online engagement only
-- Within tier: most recent attendance first, then online action volume, then
-- lifetime event count, then entity_id to break ties deterministically.
--
-- Note the tier boundary does the real work here: tier 1 alone holds ~230 people,
-- so a 150-person list never reaches tier 2. Everyone on it attended something
-- within the past year.
--
-- Grain: one row per entity, ranked. Downstream takes the top N.

{% set radius_mi = 40 %}
{% set mall_lat = 38.8895 %}
{% set mall_lon = -77.0353 %}

WITH latest_address AS (
  -- One address per entity: the most recently updated geocoded one.
  SELECT
    owner_id AS entity_id,
    state,
    postal_code,
    city,
    latitude,
    longitude,
    ROW_NUMBER() OVER (PARTITION BY owner_id ORDER BY updated_at DESC, id DESC) AS rn
  FROM actionbuilder_cleaned.cln_actionbuilder__addresses
  WHERE owner_type = 'Entity'
    AND latitude IS NOT NULL
    AND longitude IS NOT NULL
),

geo AS (
  SELECT
    entity_id,
    state,
    postal_code,
    city,
    ROUND(
      ST_DISTANCE(
        ST_GEOGPOINT(longitude, latitude),
        ST_GEOGPOINT({{ mall_lon }}, {{ mall_lat }})
      ) / 1609.34, 1
    ) AS miles_from_mall
  FROM latest_address
  WHERE rn = 1
),

-- Callable = at least one phone we could actually dial.
callable_phones AS (
  SELECT DISTINCT owner_id AS entity_id
  FROM actionbuilder_cleaned.cln_actionbuilder__phone_numbers
  WHERE owner_type = 'Entity'
    AND status IN ('verified', 'user_added')
    AND number IS NOT NULL
    AND LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(number, r'^\+', ''), r'^1', ''), r'[^\d]', '')) = 10
),

-- Any do_not_contact / bad phone disqualifies the whole entity. A person who asked
-- not to be called does not become callable by also having a second good number.
uncallable_entities AS (
  SELECT DISTINCT owner_id AS entity_id
  FROM actionbuilder_cleaned.cln_actionbuilder__phone_numbers
  WHERE owner_type = 'Entity'
    AND status IN ('do_not_contact', 'bad')
),

-- Real campaign membership: excludes the Test campaign's legacy duplicate load.
in_real_campaign AS (
  SELECT DISTINCT ce.entity_id
  FROM actionbuilder_cleaned.cln_actionbuilder__campaigns_entities ce
  JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns c ON c.id = ce.campaign_id
  WHERE c.status = 'active'
    AND c.name != 'Test'
),

organizers AS (
  SELECT DISTINCT organizer_interact_id AS interact_id
  FROM {{ ref('organizer_state_map') }}
),

pool AS (
  SELECT
    e.id            AS entity_id,
    e.interact_id   AS entity_interact_id,
    g.state,
    g.city,
    g.postal_code,
    g.miles_from_mall
  FROM geo g
  JOIN actionbuilder_cleaned.cln_actionbuilder__entities e ON e.id = g.entity_id
  JOIN callable_phones cp   ON cp.entity_id = g.entity_id
  JOIN in_real_campaign irc ON irc.entity_id = g.entity_id
  LEFT JOIN uncallable_entities unc ON unc.entity_id = g.entity_id
  LEFT JOIN {{ ref('suppressed_entities') }} sup ON sup.entity_interact_id = e.interact_id
  LEFT JOIN organizers org ON org.interact_id = e.interact_id
  WHERE g.miles_from_mall <= {{ radius_mi }}
    AND g.state IN ('DC', 'MD', 'VA')
    AND unc.entity_id IS NULL
    AND sup.entity_interact_id IS NULL
    AND org.interact_id IS NULL
),

entity_emails AS (
  SELECT
    owner_id AS entity_id,
    LOWER(TRIM(email)) AS email_normalized
  FROM actionbuilder_cleaned.cln_actionbuilder__emails
  WHERE owner_type = 'Entity'
    AND status IN ('verified', 'user_added')
    AND email IS NOT NULL
),

entity_phones AS (
  SELECT
    owner_id AS entity_id,
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(number, r'^\+', ''), r'^1', ''), r'[^\d]', '') AS number_normalized
  FROM actionbuilder_cleaned.cln_actionbuilder__phone_numbers
  WHERE owner_type = 'Entity'
    AND status IN ('verified', 'user_added')
    AND number IS NOT NULL
),

-- Event attendance: the tier signal. attended = True only — an RSVP is not a turnout.
-- (See MEMORY: events_attended_past_6_months does NOT filter attended and runs ~3.3x
-- real attendance; do not reuse that metric here.)
mobilize_attendance AS (
  SELECT
    ee.entity_id,
    COUNT(DISTINCT mp.event_id) AS events_attended,
    MAX(DATE(COALESCE(mp.utc_override_start_date, mp.utc_start_date))) AS last_event_date
  FROM entity_emails ee
  JOIN mobilize_cleaned.cln_mobilize__participations mp
    ON LOWER(TRIM(mp.user__email_address)) = ee.email_normalized
  WHERE mp.attended = True
  GROUP BY 1
),

an_actions AS (
  SELECT ee.entity_id, SUM(a.total_actions_6_months) AS an_actions_6mo
  FROM entity_emails ee
  JOIN {{ ref('action_network_6mo_actions') }} a ON a.email_normalized = ee.email_normalized
  GROUP BY 1
),

newmode_actions AS (
  SELECT ee.entity_id, COUNT(DISTINCT n.submission_id) AS newmode_actions
  FROM entity_emails ee
  JOIN newmode_cleaned.cln_newmode__submissions n
    ON LOWER(TRIM(n.contact_email)) = ee.email_normalized
  WHERE n.testmode IS DISTINCT FROM TRUE
  GROUP BY 1
),

soapboxx AS (
  SELECT ee.entity_id, SUM(s.soapboxx_stories) AS soapboxx_stories
  FROM entity_emails ee
  JOIN {{ ref('soapboxx_stories') }} s ON s.email_normalized = ee.email_normalized
  GROUP BY 1
),

stw AS (
  SELECT ep.entity_id, SUM(c.phone_bank_calls_made) AS phone_bank_calls
  FROM entity_phones ep
  JOIN {{ ref('scaletowin_call_data') }} c ON c.caller_phone_number = ep.number_normalized
  GROUP BY 1
),

scored AS (
  SELECT
    p.entity_id,
    p.entity_interact_id,
    p.state,
    p.city,
    p.postal_code,
    p.miles_from_mall,
    COALESCE(ma.events_attended, 0)     AS events_attended,
    ma.last_event_date,
    COALESCE(an.an_actions_6mo, 0)      AS an_actions_6mo,
    COALESCE(nm.newmode_actions, 0)     AS newmode_actions,
    COALESCE(sb.soapboxx_stories, 0)    AS soapboxx_stories,
    COALESCE(st.phone_bank_calls, 0)    AS phone_bank_calls,
    CASE
      WHEN ma.last_event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY) THEN 1
      WHEN COALESCE(ma.events_attended, 0) > 0 THEN 2
      ELSE 3
    END AS tier,
    COALESCE(an.an_actions_6mo, 0)
      + COALESCE(nm.newmode_actions, 0)
      + COALESCE(sb.soapboxx_stories, 0)
      + COALESCE(st.phone_bank_calls, 0) AS online_activity_score
  FROM pool p
  LEFT JOIN mobilize_attendance ma ON ma.entity_id = p.entity_id
  LEFT JOIN an_actions an          ON an.entity_id = p.entity_id
  LEFT JOIN newmode_actions nm     ON nm.entity_id = p.entity_id
  LEFT JOIN soapboxx sb            ON sb.entity_id = p.entity_id
  LEFT JOIN stw st                 ON st.entity_id = p.entity_id
)

SELECT
  s.*,
  CASE s.tier
    WHEN 1 THEN 'Attended in last 12 months'
    WHEN 2 THEN 'Attended ever'
    ELSE 'Online engagement only'
  END AS tier_label,
  ROW_NUMBER() OVER (
    ORDER BY
      s.tier,
      s.last_event_date DESC NULLS LAST,
      s.online_activity_score DESC,
      s.events_attended DESC,
      s.entity_id
  ) AS target_rank
FROM scored s
ORDER BY target_rank
