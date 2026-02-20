WITH primary_emails AS (
  -- Get the primary email for each entity
  SELECT 
    owner_id as entity_id,
    email,
    ROW_NUMBER() OVER (
      PARTITION BY owner_id 
      ORDER BY 
        CASE WHEN status = 'verified' THEN 1 
             WHEN status = 'user_added' THEN 2 
             ELSE 3 END,
        updated_at DESC
    ) as email_rank
  FROM actionbuilder_cleaned.cln_actionbuilder__emails
  WHERE owner_type = 'Entity'
    AND status IN ('verified', 'user_added')
    AND email IS NOT NULL
),

primary_phones AS (
  -- Get the primary phone for each entity
  SELECT 
    owner_id as entity_id,
    number,
    ROW_NUMBER() OVER (
      PARTITION BY owner_id 
      ORDER BY 
        CASE WHEN status = 'verified' THEN 1 
             WHEN status = 'user_added' THEN 2 
             ELSE 3 END,
        updated_at DESC
    ) as phone_rank
  FROM actionbuilder_cleaned.cln_actionbuilder__phone_numbers
  WHERE owner_type = 'Entity'
    AND status IN ('verified', 'user_added')
    AND number IS NOT NULL
)

SELECT 
  e.interact_id,
  e.id as entity_id,
  e.first_name,
  e.last_name,
  CONCAT(COALESCE(e.first_name, ''), ' ', COALESCE(e.last_name, '')) as full_name,
  pe.email,
  pp.number as phone_number,
  e.created_at,
  e.updated_at
  
FROM actionbuilder_cleaned.cln_actionbuilder__entities e
LEFT JOIN primary_emails pe
  ON e.id = pe.entity_id AND pe.email_rank = 1
LEFT JOIN primary_phones pp
  ON e.id = pp.entity_id AND pp.phone_rank = 1
  
ORDER BY e.interact_id