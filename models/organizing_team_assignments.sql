-- organizing_team_assignments.sql
-- Routes each Organizing Team (campaign 26) member to the C&O National Organizing
-- Team organizer who covers their state, so the assign_organizers sync op can wire
-- a People:People AB connection (member -> organizer) tagged "Regional Organizer".
--
-- The connection IS the assignment; the info tag (section "Organizer Relationships",
-- field "Assigned Organizer", value "Regional Organizer") just labels the relationship
-- so it reads clearly in the organizer's Connections tab. Which organizer is captured
-- by the connection's other endpoint, not by the (constant) tag value.
--
-- Routing: member entity -> its emails -> ofp_universe.state (zip-derived) ->
-- organizer_state_map seed. Only the 24 staffed states are in the seed, so members
-- in unstaffed or unknown states are intentionally left unassigned in v1 (they never
-- join the seed). See KL "C&O National Organizing Team — State Coverage".
--
-- Idempotency / replication lag:
--   - Members come from the BQ campaigns_entities snapshot UNION the sync_log
--     connect_entity overlay, so members connected earlier the same night (step 5)
--     are routable before BQ replicates them.
--   - Already-assigned members are anti-joined via sync_log create_connection rows,
--     so re-running never re-issues a connection (create_connection is also
--     idempotent server-side, but this keeps the feed small).
--
-- v1 is create-only: a member whose state later changes keeps their first organizer
-- (they're already in create_connection log). Reassignment (inactivate-old +
-- create-new via update_connection) is a future enhancement.
--
-- Grain: one row per member entity (one organizer each).

{% set campaign_26 = '1e7e58fd-efb4-4810-91dc-2e7aac08625a' %}

WITH
-- Members of campaign 26 per the BQ snapshot
members_bq AS (
  SELECT DISTINCT e.interact_id AS member_interact_id
  FROM actionbuilder_cleaned.cln_actionbuilder__campaigns_entities ce
  JOIN actionbuilder_cleaned.cln_actionbuilder__entities e ON e.id = ce.entity_id
  WHERE ce.campaign_id = 26
),

-- Members connected to 26 via our sync_log (covers step-5 connects not yet replicated)
members_log AS (
  SELECT DISTINCT entity_interact_id AS member_interact_id
  FROM `proj-tmc-mem-com`.actionbuilder_sync.sync_log
  WHERE operation = 'connect_entity'
    AND status IN ('ok', '404')
    AND campaign_interact_id = '{{ campaign_26 }}'
    AND entity_interact_id IS NOT NULL
),

members AS (
  SELECT member_interact_id FROM members_bq
  UNION DISTINCT
  SELECT member_interact_id FROM members_log
),

-- Resolve each member entity to its verified emails
member_emails AS (
  SELECT
    e.interact_id           AS member_interact_id,
    LOWER(TRIM(em.email))   AS email
  FROM members m
  JOIN actionbuilder_cleaned.cln_actionbuilder__entities e
    ON e.interact_id = m.member_interact_id
  JOIN actionbuilder_cleaned.cln_actionbuilder__emails em
    ON em.owner_id = e.id
   AND em.owner_type = 'Entity'
   AND em.status IN ('verified', 'user_added')
  WHERE em.email IS NOT NULL
),

-- Zip-derived state per member, via ofp_universe (they're all OFP attendees)
member_state AS (
  SELECT DISTINCT me.member_interact_id, u.state
  FROM member_emails me
  JOIN {{ ref('ofp_universe') }} u ON u.email_normalized = me.email
  WHERE u.state IS NOT NULL
),

-- Route to the assigned organizer; one organizer per member (deterministic on the
-- rare multi-email/multi-state member)
routed AS (
  SELECT
    ms.member_interact_id,
    ms.state,
    map.organizer_interact_id,
    map.organizer_name
  FROM member_state ms
  JOIN {{ ref('organizer_state_map') }} map ON map.state = ms.state
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ms.member_interact_id
    ORDER BY ms.state
  ) = 1
),

-- Members already assigned per our sync_log (idempotency + replication lag)
already_assigned AS (
  SELECT DISTINCT entity_interact_id AS member_interact_id
  FROM `proj-tmc-mem-com`.actionbuilder_sync.sync_log
  WHERE operation = 'create_connection'
    AND status IN ('ok', '404')
    AND campaign_interact_id = '{{ campaign_26 }}'
    AND entity_interact_id IS NOT NULL
),

-- The organizers themselves must never be assigned to an organizer
organizers AS (
  SELECT DISTINCT organizer_interact_id AS interact_id
  FROM {{ ref('organizer_state_map') }}
)

SELECT
  '{{ campaign_26 }}'        AS campaign_interact_id,
  r.member_interact_id,
  r.organizer_interact_id,
  r.organizer_name,
  r.state,
  -- Connection info tag (standard_response single-select).
  'Organizer Relationships:|:Assigned Organizer:|:Regional Organizer:|:standard_response:Regional Organizer'
    AS sync_string
FROM routed r
LEFT JOIN already_assigned aa ON aa.member_interact_id = r.member_interact_id
LEFT JOIN organizers        og ON og.interact_id       = r.member_interact_id
WHERE aa.member_interact_id IS NULL
  AND og.interact_id IS NULL
ORDER BY r.organizer_name, r.state, r.member_interact_id
