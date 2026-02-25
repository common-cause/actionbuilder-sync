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
    WHEN tag_name = 'Events Attended Past 6 Months'          THEN 'Participation:|:Event Attendance Summary:|:Events Attended Past 6 Months:|:number_response'
    WHEN tag_name = 'Most Recent Event Attended'              THEN 'Participation:|:Event Attendance History:|:Most Recent Event Attended:|:date_response'
    WHEN tag_name = 'First Event Attended'                    THEN 'Participation:|:Event Attendance History:|:First Event Attended:|:date_response'
    WHEN tag_name = 'Action Network Actions'                  THEN 'Participation:|:Online Actions Past 6 Months:|:Action Network Actions:|:number_response'
    WHEN tag_name = 'Action Network State Actions'            THEN 'Participation:|:Online Actions Past 6 Months:|:Action Network State Actions:|:number_response'
    WHEN tag_name = 'Top State Action Taker'                  THEN 'Participation:|:State Online Actions:|:Top State Action Taker:|:standard_response'
    WHEN tag_name = 'Phone Bank Calls Made'                   THEN 'Participation:|:Event Attendance Summary:|:Phone Bank Calls Made:|:number_response'
    WHEN tag_name = 'NewMode Actions'                         THEN 'Participation:|:Online Actions Past 6 Months:|:NewMode Actions:|:number_response'
    WHEN tag_name = 'Top National Action Network Activist'    THEN 'Participation:|:National Online Actions:|:Top National Action Network Activist:|:standard_response'
    WHEN tag_name = 'Hot Prospect'                            THEN 'Engagement:|:Prospect Identification:|:Hot Prospect:|:standard_response'
    ELSE CONCAT(tag_name, ':|:', tag_type, '_response')
  END as sync_field_identifier

FROM tag_values_with_notes
ORDER BY entity_id, campaign_id, tag_name
