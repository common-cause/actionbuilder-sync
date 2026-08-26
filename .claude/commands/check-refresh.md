Check whether the ActionBuilder BQ tables have refreshed since the last sync run.

Run the following queries in parallel and report results:

1. **campaigns_entities freshness:**
```sql
SELECT MAX(updated_at) as latest_update, COUNT(*) as total_rows
FROM actionbuilder_cleaned.cln_actionbuilder__campaigns_entities
```

2. **taggable_logbook freshness:**
```sql
SELECT MAX(updated_at) as latest_update, COUNT(*) as total_rows
FROM actionbuilder_cleaned.cln_actionbuilder__taggable_logbook
```

3. **Recent entity creation breakdown:**
```sql
SELECT DATE(created_at) as created_date, COUNT(*) as new_entities
FROM actionbuilder_cleaned.cln_actionbuilder__campaigns_entities
WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 DAY)
GROUP BY 1
ORDER BY 1 DESC
```

4. **View counts (quick sanity check):**
```sql
SELECT
  (SELECT COUNT(*) FROM actionbuilder_sync.dedup_candidates) as dedup_candidates,
  (SELECT COUNT(*) FROM actionbuilder_sync.deduplicated_names_to_load_bq_only) as names_to_load_bq_only,
  (SELECT COUNT(*) FROM actionbuilder_sync.deduplicated_names_to_load) as names_to_load_filtered
```

5. **Spot-check entities with apparent duplicate tags (if relevant):**
```sql
SELECT
  e.id as entity_id,
  e.first_name,
  e.last_name,
  c.id as campaign_id,
  c.name as campaign_name,
  t.name as tag_name,
  COUNT(*) as duplicate_count
FROM actionbuilder_cleaned.cln_actionbuilder__taggable_logbook tl
JOIN actionbuilder_cleaned.cln_actionbuilder__tags t ON tl.tag_id = t.id
JOIN actionbuilder_cleaned.cln_actionbuilder__entities e ON tl.taggable_id = e.id
JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns_entities ce ON e.id = ce.entity_id
JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns c ON ce.campaign_id = c.id
WHERE tl.taggable_type = 'Entity'
  AND tl.deleted_at IS NULL
  AND tl.available = TRUE
  AND t.status = 1
GROUP BY 1, 2, 3, 4, 5, 6
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 5
```

For any entities returned, profile URL format is:
`https://commoncause.actionbuilder.org/entity/view/{entity_id}/profile?campaignId={campaign_id}&clientQueryId=null`

Then summarize:
- Whether campaigns_entities has refreshed today (and how many new entities created recently)
- Whether taggable_logbook is current. Replication was **restored ~2026-03-21** (it had been stuck at 2026-03-05), so a recent `latest_update` is the expected healthy state now — treat a stale one as a new incident, not the known bug.
- Current dedup_candidates count and whether any are actionable (have a campaign_interact_id)
- Current names_to_load count and whether it reflects expected post-insert drop
- Any notable discrepancies between bq_only and filtered view counts
- Duplicate-tag counts from query 5 are **unreliable regardless of replication health**: our own tag deletes never set `deleted_at` in BQ, so roughly 90% of rows that look live are ghosts. Always join against `sync_log` `delete_tagging` rows before reporting a duplicate-tagging count.
