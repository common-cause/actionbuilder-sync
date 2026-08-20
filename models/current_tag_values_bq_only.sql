WITH latest_tag_applications AS (
  -- Get the most recent application of each tag for each entity/campaign combo
  SELECT
    tl.taggable_id as entity_id,
    tl.campaign_id,
    tl.tag_id,
    tl.id as taggable_logbook_id,
    tl.interact_id as taggable_logbook_interact_id,
    tl.created_at,
    ROW_NUMBER() OVER (
      PARTITION BY tl.taggable_id, tl.campaign_id, tl.tag_id
      ORDER BY tl.created_at DESC
    ) as rn
  FROM actionbuilder_cleaned.cln_actionbuilder__taggable_logbook tl
  WHERE tl.taggable_type = 'Entity'
    AND tl.deleted_at IS NULL
    AND tl.available = True
),

current_tag_applications AS (
  -- Filter to only the most recent application
  SELECT
    entity_id,
    campaign_id,
    tag_id,
    taggable_logbook_id,
    taggable_logbook_interact_id,
    created_at
  FROM latest_tag_applications
  WHERE rn = 1
),

tag_values_with_notes AS (
  -- Join with tags and global_notes to get the actual values
  SELECT
    cta.entity_id,
    cta.campaign_id,
    cta.tag_id,
    t.interact_id as tag_interact_id,
    t.name as tag_name,
    t.tag_type,
    t.tag_category_id,
    cta.taggable_logbook_id,
    cta.taggable_logbook_interact_id,
    cta.created_at as tag_applied_at,

    -- Get the value from global_notes if it exists
    gn.text as tag_value,
    gn.note_type,

    -- For standard tags (no value needed), just mark as applied
    CASE
      WHEN t.tag_type = 'Standard' THEN 'applied'
      WHEN gn.text IS NOT NULL THEN gn.text
      ELSE NULL
    END as current_value

  FROM current_tag_applications cta
  INNER JOIN actionbuilder_cleaned.cln_actionbuilder__tags t
    ON cta.tag_id = t.id
  LEFT JOIN actionbuilder_cleaned.cln_actionbuilder__global_notes gn
    ON gn.owner_id = cta.taggable_logbook_id
    AND gn.owner_type = 'TaggableLogbook'

  WHERE t.status = 1  -- Only active tags
    -- Taxonomy Block H0 (2026-08-20): drop the four LEGACY twins of the Block G
    -- value names. These four names each exist twice with status = 1 -- once in
    -- the legacy Participation section, once in the Block B/C home -- and every
    -- read below (the sync_field_identifier CASE, updates_needed's tag_name
    -- pivots, the overlay's name join) keys on the NAME, so a legacy tagging was
    -- being reported as the new field. Measured on the cutover night: the two
    -- universal Activity dates got 17 and 35 writes instead of ~20,000 each
    -- because ~20K legacy taggings made them look already-correct, and the Top
    -- Performer rotation deleted the LEGACY taggings (399 on tag 64, 45 on 74)
    -- while the new Engagement copies accumulated with no removal path.
    --
    -- Nothing writes these four any more (Block G moved every feed), so excluding
    -- them here retires them from the pipeline in one place. Do NOT try to solve
    -- this by archiving them in AB: value archival does not reliably replicate to
    -- cln_actionbuilder__tags.status (see CLAUDE.md, BQ replication gap #3).
    --   40  Participation > Event Attendance History  > First Event Attended
    --   41  Participation > Event Attendance History  > Most Recent Event Attended
    --   64  Participation > State Online Actions      > Top State Action Taker
    --   74  Participation > National Online Actions   > Top National Action Network Activist
    AND t.id NOT IN (40, 41, 64, 74)
)

SELECT
  entity_id,
  campaign_id,
  tag_id,
  tag_interact_id,
  tag_name,
  tag_type,
  tag_category_id,
  taggable_logbook_interact_id,
  current_value,
  tag_applied_at,

  -- Pre-built removal string: tag-interact-id:|:tagging-interact-id
  -- Used by the sync job to remove an existing tag value before setting a new one
  CONCAT(tag_interact_id, ':|:', taggable_logbook_interact_id) as removal_string,

  -- Create formatted field identifier for sync strings
  CASE
    -- Taxonomy Block G (2026-08-19): the 8 activity metrics live in the universal
    -- Activity section (field name == response name); the two top-performer flags
    -- live in campaign-local Engagement > Top Performers.
    WHEN tag_name = 'Events Attended (Past 6 Months)'               THEN 'Activity:|:Events Attended (Past 6 Months):|:Events Attended (Past 6 Months):|:number_response'
    WHEN tag_name = 'Most Recent Event Attended'                    THEN 'Activity:|:Most Recent Event Attended:|:Most Recent Event Attended:|:date_response'
    WHEN tag_name = 'First Event Attended'                          THEN 'Activity:|:First Event Attended:|:First Event Attended:|:date_response'
    WHEN tag_name = 'Action Network Actions (Past 6 Months)'         THEN 'Activity:|:Action Network Actions (Past 6 Months):|:Action Network Actions (Past 6 Months):|:number_response'
    WHEN tag_name = 'State Action Network Actions (Past 6 Months)'   THEN 'Activity:|:State Action Network Actions (Past 6 Months):|:State Action Network Actions (Past 6 Months):|:number_response'
    WHEN tag_name = 'Top State Action Taker'                        THEN 'Engagement:|:Top Performers:|:Top State Action Taker:|:standard_response'
    WHEN tag_name = 'Phone Bank Calls Made (All Time)'              THEN 'Activity:|:Phone Bank Calls Made (All Time):|:Phone Bank Calls Made (All Time):|:number_response'
    WHEN tag_name = 'NewMode Actions (All Time)'                    THEN 'Activity:|:NewMode Actions (All Time):|:NewMode Actions (All Time):|:number_response'
    WHEN tag_name = 'Soapboxx Stories (All Time)'                   THEN 'Activity:|:Soapboxx Stories (All Time):|:Soapboxx Stories (All Time):|:number_response'
    WHEN tag_name = 'Top National Action Network Activist'          THEN 'Engagement:|:Top Performers:|:Top National Action Network Activist:|:standard_response'
    WHEN tag_name = 'Hot Prospect'                            THEN 'Engagement:|:Prospect Identification:|:Hot Prospect:|:standard_response'
    WHEN tag_name = 'OFP Training: Organizing Basics'         THEN 'Trainings:|:Organizing For Power:|:OFP Training: Organizing Basics:|:standard_response'
    WHEN tag_name = 'OFP Training: Storytelling'              THEN 'Trainings:|:Organizing For Power:|:OFP Training: Storytelling:|:standard_response'
    WHEN tag_name = 'OFP Training: Relational Organizing'     THEN 'Trainings:|:Organizing For Power:|:OFP Training: Relational Organizing:|:standard_response'
    WHEN tag_name = 'OFP Training: Rapid Response Basics'     THEN 'Trainings:|:Organizing For Power:|:OFP Training: Rapid Response Basics:|:standard_response'
    ELSE CONCAT(tag_name, ':|:', tag_type, '_response')
  END as sync_field_identifier

FROM tag_values_with_notes
ORDER BY entity_id, campaign_id, tag_name
