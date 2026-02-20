WITH actionbuilder_emails AS (
  -- Get primary emails for ActionBuilder entities
  SELECT 
    e.owner_id as entity_id,
    LOWER(TRIM(e.email)) as email_normalized,
    e.email as email_raw,
    ROW_NUMBER() OVER (
      PARTITION BY e.owner_id 
      ORDER BY 
        CASE WHEN e.status = 'verified' THEN 1 
             WHEN e.status = 'user_added' THEN 2 
             ELSE 3 END,
        e.updated_at DESC
    ) as email_rank
  FROM actionbuilder_cleaned.cln_actionbuilder__emails e
  WHERE e.owner_type = 'Entity'
    AND e.status IN ('verified', 'user_added')
    AND e.email IS NOT NULL
),

actionbuilder_phones AS (
  -- Get primary phone numbers for ActionBuilder entities
  SELECT 
    p.owner_id as entity_id,
    REGEXP_REPLACE(p.number, r'[^\d]', '') as phone_normalized,
    p.number as phone_raw,
    ROW_NUMBER() OVER (
      PARTITION BY p.owner_id 
      ORDER BY 
        CASE WHEN p.status = 'verified' THEN 1 
             WHEN p.status = 'user_added' THEN 2 
             ELSE 3 END,
        p.updated_at DESC
    ) as phone_rank
  FROM actionbuilder_cleaned.cln_actionbuilder__phone_numbers p
  WHERE p.owner_type = 'Entity'
    AND p.status IN ('verified', 'user_added')
    AND p.number IS NOT NULL
),

primary_actionbuilder_contacts AS (
  -- Get primary email and phone for each ActionBuilder entity
  SELECT 
    e.entity_id,
    e.email_normalized as primary_email,
    p.phone_normalized as primary_phone
  FROM actionbuilder_emails e
  LEFT JOIN actionbuilder_phones p 
    ON e.entity_id = p.entity_id 
    AND p.phone_rank = 1
  WHERE e.email_rank = 1
  
  UNION ALL
  
  -- Include entities that only have phone numbers (no email)
  SELECT 
    p.entity_id,
    NULL as primary_email,
    p.phone_normalized as primary_phone
  FROM actionbuilder_phones p
  LEFT JOIN actionbuilder_emails e 
    ON p.entity_id = e.entity_id 
    AND e.email_rank = 1
  WHERE p.phone_rank = 1
    AND e.entity_id IS NULL  -- Only entities without primary email
),

email_to_person_mapping AS (
  -- Map emails to person_ids
  SELECT DISTINCT
    LOWER(TRIM(email)) as email_normalized,
    person_id,
    original_person_id
  FROM core_enhanced.enh_activistpools__emails
  WHERE email IS NOT NULL
),

phone_to_person_mapping AS (
  -- Map phones to person_ids (normalize phone format)
  SELECT DISTINCT
    REGEXP_REPLACE(phone_number, r'[^\d]', '') as phone_normalized,
    person_id,
    original_person_id
  FROM core_enhanced.enh_activistpools__phones
  WHERE phone_number IS NOT NULL
),

actionbuilder_to_person AS (
  -- Link ActionBuilder entities to person_ids via email and phone
  SELECT 
    ab.entity_id,
    ab.primary_email,
    ab.primary_phone,
    
    -- Try to get person_id via email first, then phone
    COALESCE(ep.person_id, pp.person_id) as person_id,
    COALESCE(ep.original_person_id, pp.original_person_id) as original_person_id,
    
    -- Track which method was used for linkage
    CASE 
      WHEN ep.person_id IS NOT NULL THEN 'email'
      WHEN pp.person_id IS NOT NULL THEN 'phone'
      ELSE 'no_match'
    END as match_method
    
  FROM primary_actionbuilder_contacts ab
  LEFT JOIN email_to_person_mapping ep
    ON ab.primary_email = ep.email_normalized
  LEFT JOIN phone_to_person_mapping pp
    ON ab.primary_phone = pp.phone_normalized
),

mobilize_person_mapping AS (
  -- Map Mobilize emails to person_ids
  SELECT DISTINCT
    LOWER(TRIM(user__email_address)) as mobilize_email,
    ep.person_id
  FROM mobilize_cleaned.cln_mobilize__participations m
  INNER JOIN email_to_person_mapping ep
    ON LOWER(TRIM(m.user__email_address)) = ep.email_normalized
  WHERE m.user__email_address IS NOT NULL
),

scaletowin_person_mapping AS (
  -- Map ScaleToWin caller numbers to person_ids
  SELECT DISTINCT
    REGEXP_REPLACE(caller_number, r'[^\d]', '') as caller_phone_normalized,
    pp.person_id
  FROM scaletowin_dialer_cleaned.cln_scaletowin_dialer__calls s
  INNER JOIN phone_to_person_mapping pp
    ON REGEXP_REPLACE(s.caller_number, r'[^\d]', '') = pp.phone_normalized
  WHERE s.caller_number IS NOT NULL
)

-- Final comprehensive mapping
SELECT 
DISTINCT
  abp.entity_id,
  --abp.person_id, take this out until we figure out the dupe situation here
  abp.primary_email,
  abp.primary_phone,
  abp.match_method,
  
  -- Include entity details
  e.first_name,
  e.last_name,
  CONCAT(COALESCE(e.first_name, ''), ' ', COALESCE(e.last_name, '')) as full_name,
  
  -- Track which systems this person appears in
  CASE WHEN mp.person_id IS NOT NULL THEN TRUE ELSE FALSE END as has_mobilize_data,
  CASE WHEN sp.person_id IS NOT NULL THEN TRUE ELSE FALSE END as has_scaletowin_data,
  CASE WHEN abp.person_id IS NOT NULL THEN TRUE ELSE FALSE END as has_person_id

FROM actionbuilder_to_person abp
INNER JOIN actionbuilder_cleaned.cln_actionbuilder__entities e
  ON abp.entity_id = e.id
LEFT JOIN mobilize_person_mapping mp
  ON abp.person_id = mp.person_id
LEFT JOIN scaletowin_person_mapping sp  
  ON abp.person_id = sp.person_id

ORDER BY abp.entity_id