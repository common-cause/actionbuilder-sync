-- organizing_team_review.sql
-- OFP attendees who cannot be routed cleanly into the Organizing Team campaign (26)
-- and need human review instead of being silently mis-handled.
--
-- Primary case: an attendee who exists in ActionBuilder but ONLY in non-state campaigns
-- (e.g. the Test campaign's legacy duplicate load, or only campaign 26 with no underlying
-- state-campaign entity). They are not connectable (the connect pool is state campaigns
-- only) and not insertable (they already exist in AB), so they deadlock. The known fix is
-- a one-time manual insert into the correct state campaign, after which the connect feed
-- picks them up automatically.
--
-- Grain: one row per (email, entity), listing the campaign(s) the entity sits in.

WITH ofp_people AS (
  SELECT DISTINCT email_normalized
  FROM {{ ref('ofp_universe') }}
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

-- Every campaign each OFP-matched entity belongs to
matched_campaigns AS (
  SELECT
    op.email_normalized,
    e.id           AS entity_id,
    e.interact_id  AS entity_interact_id,
    c.id           AS campaign_id,
    c.name         AS campaign_name
  FROM ofp_people op
  JOIN entity_emails ee ON ee.email_normalized = op.email_normalized
  JOIN actionbuilder_cleaned.cln_actionbuilder__entities e ON e.id = ee.entity_id
  JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns_entities ce ON ce.entity_id = e.id
  JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns c ON c.id = ce.campaign_id
  WHERE c.status = 'active'
),

-- Per matched entity: does it have any state-campaign membership, and is it in 26?
entity_flags AS (
  SELECT
    email_normalized,
    entity_id,
    entity_interact_id,
    STRING_AGG(DISTINCT campaign_name, ', ' ORDER BY campaign_name) AS campaigns,
    LOGICAL_OR(campaign_name NOT IN ('Test', 'Organizing Team')) AS has_state_campaign,
    LOGICAL_OR(campaign_id = 26)                                  AS in_campaign_26
  FROM matched_campaigns
  GROUP BY email_normalized, entity_id, entity_interact_id
)

SELECT
  email_normalized,
  entity_interact_id,
  campaigns,
  'In AB but no state-campaign entity; cannot connect or insert' AS review_reason
FROM entity_flags
WHERE has_state_campaign = FALSE
  AND in_campaign_26 = FALSE
ORDER BY email_normalized
