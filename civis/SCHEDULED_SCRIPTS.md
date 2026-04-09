# Scheduled Scripts — ActionBuilder Sync

*Last verified: 2026-04-09*

## Workflow

### Nightly ActionBuilder Update
- **Civis workflow:** [#119217](https://platform.civisanalytics.com/spa/#/workflows/119217)
- **Schedule:** Daily at 10:00 PM ET
- **Typical runtime:** 1.5–4 hours (observed)
- **Steps (sequential):**
  1. AB Inserts → `insert_new_records.sh`
  2. AB Tag Updates → `update_records.sh`
  3. AB Assessment Setting → `apply_assessments.sh`
  4. AB Notes Append → `append_notes.sh`

## Nightly Scripts

### insert_new_records.sh — AB Inserts
- **Civis script:** [#345082775](https://platform.civisanalytics.com/spa/#/scripts/containers/345082775)
- **Workflow step:** 1
- **APIs:** ActionBuilder API (~4 req/sec, throttled 0.3s), BigQuery (read)
- **Input view:** `actionbuilder_sync.deduplicated_names_to_load`
- **Description:** Creates new entities in ActionBuilder from unmatched activists. Runs across all 24 state campaigns sequentially with --delay 0.3. Full dedup guards: AB person_id match, email match, phone-only dedup, test account filter, within-feed dedup, sync_log filter, first_name NOT NULL filter.

### update_records.sh — AB Tag Updates
- **Civis script:** [#346138397](https://platform.civisanalytics.com/#/scripts/containers/346138397)
- **Workflow step:** 2
- **APIs:** ActionBuilder API (~4 req/sec, throttled 0.3s), BigQuery (read)
- **Input view:** `actionbuilder_sync.updates_needed`
- **Description:** Syncs tag values from BigQuery to ActionBuilder. Runs across all 24 campaigns with --delay 0.3. Removes old tag values (DELETE, 404-tolerant) then writes new values (POST via Person Signup Helper).

### apply_assessments.sh — AB Assessment Setting
- **Civis script:** [#346528478](https://platform.civisanalytics.com/#/scripts/containers/346528478)
- **Workflow step:** 3
- **APIs:** ActionBuilder API (~4 req/sec, throttled 0.3s), BigQuery (read)
- **Input view:** `actionbuilder_sync.auto_assessment_rules`
- **Description:** Sets engagement assessment levels automatically (upgrade-only). Level 1: Mobilize attendance, NewMode submission, 2+ STW calls, or 20+ AN actions in 6mo. Level 2: 2+ STW calls, 2+ virtual Mobilize, or any in-person CC Mobilize event. Level 3: 1MC Leader tag.

### append_notes.sh — AB Notes Append
- **Civis script:** [#348368977](https://platform.civisanalytics.com/#/scripts/containers/348368977)
- **Workflow step:** 4
- **APIs:** ActionBuilder API (~4 req/sec, throttled 0.3s), BigQuery (read)
- **Input view:** `actionbuilder_sync.1mc_notes`
- **Description:** Appends 1MC conversation notes to entities (Event Host Notes, Conversation Host Notes, Event Attendee Notes). Idempotent via sync_log (keyed on airtable_record_id + response_name).

## On-Demand Scripts

### snapshot_tag_state.sh
- **Type:** On-demand (not in nightly workflow)
- **APIs:** ActionBuilder API (~4 req/sec, throttled 0.3s), BigQuery (write)
- **Description:** Captures current tag state from AB API for all campaigns and logs `add_tagging` rows to `actionbuilder_sync.sync_log`. Used for recovery/healing sync_log gaps — critical to run before retrying `update_records` after a failed sync.

### cleanup_duplicate_tags.sh
- **Type:** On-demand (not in nightly workflow)
- **APIs:** ActionBuilder API (~4 req/sec, throttled 0.3s)
- **Description:** Removes duplicate tag values from entities across all campaigns. Calls DELETE on individual tag-entity associations. Originally a post-dedup operation; available for periodic use.

### remove_duplicate_entities.sh
- **Type:** On-demand (completed March 2026, not currently scheduled)
- **APIs:** ActionBuilder API (~4 req/sec, throttled 0.3s), BigQuery (read)
- **Description:** Three-phase dedup: migrate secondary emails to keeper entities, migrate phone numbers, then delete duplicate entities. Completed March 2026 (374 duplicates resolved). Retained for future use if duplicates reappear.
