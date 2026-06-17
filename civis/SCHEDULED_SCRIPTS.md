# Scheduled Scripts — ActionBuilder Sync

*Last verified: 2026-06-16*

## Job Setup (all scripts)

All Civis jobs are **GitHub-backed**: each job has the repo
`common-cause/actionbuilder-sync` (ref `master`) attached, Civis clones it into
`app/`, and the job body in the Civis UI is just a stub:

```bash
bash app/civis/<script>.sh
```

The real setup/run steps live in the version-controlled `civis/*.sh` files —
edit those (and push) to change what runs in Civis; never edit script bodies in
the Civis UI. Each script pip-installs `ccef-connections[bigquery]` from GitHub
at run time, **pinned to a release tag** (currently `@v0.2.0`) so library pushes
to master never change these jobs — bump the pin in the `.sh` files deliberately
when upgrading. (`python-dotenv` comes with it as a base dependency.)

## Workflows

### Nightly ActionBuilder Update
- **Civis name:** Nightly ActionBuilder Update — [workflow #119217](https://platform.civisanalytics.com/spa/#/workflows/119217)
- **Schedule:** Daily at 10:00 PM ET
- **Typical runtime:** 1.5–4 hours (observed)
- **Steps:** insert_new_records.sh → update_records.sh → apply_assessments.sh → append_notes.sh → connect_entities.sh → insert_organizing_team.sh
- Step names in Civis: AB Inserts / AB Tag Updates / AB Assessment Setting / AB Notes Append / AB Organizing Team Connect / AB Organizing Team Inserts (sequential)

## Scripts

### insert_new_records.sh — AB Inserts
- **Type:** Scheduled (via Nightly ActionBuilder Update, step 1)
- **Civis script:** [#345082775](https://platform.civisanalytics.com/spa/#/scripts/containers/345082775)
- **APIs:** ActionBuilder API (~4 req/sec, throttled 0.3s), BigQuery (read)
- **Input view:** `actionbuilder_sync.deduplicated_names_to_load`
- **Description:** Creates new entities in ActionBuilder from unmatched activists. Runs across all 24 state campaigns sequentially with --delay 0.3. Full dedup guards: AB person_id match, email match, phone-only dedup, test account filter, within-feed dedup, sync_log filter, first_name NOT NULL filter.

### update_records.sh — AB Tag Updates
- **Type:** Scheduled (via Nightly ActionBuilder Update, step 2)
- **Civis script:** [#346138397](https://platform.civisanalytics.com/#/scripts/containers/346138397)
- **APIs:** ActionBuilder API (~4 req/sec, throttled 0.3s), BigQuery (read)
- **Input view:** `actionbuilder_sync.updates_needed`
- **Description:** Syncs tag values from BigQuery to ActionBuilder. Runs across all 24 campaigns with --delay 0.3. Removes old tag values (DELETE, 404-tolerant) then writes new values (POST via Person Signup Helper).

### apply_assessments.sh — AB Assessment Setting
- **Type:** Scheduled (via Nightly ActionBuilder Update, step 3)
- **Civis script:** [#346528478](https://platform.civisanalytics.com/#/scripts/containers/346528478)
- **APIs:** ActionBuilder API (~4 req/sec, throttled 0.3s), BigQuery (read)
- **Input view:** `actionbuilder_sync.auto_assessment_rules`
- **Description:** Sets engagement assessment levels automatically (upgrade-only). Level 1: Mobilize attendance, NewMode submission, 2+ STW calls, or 20+ AN actions in 6mo. Level 2: 2+ STW calls, 2+ virtual Mobilize, or any in-person CC Mobilize event. Level 3: 1MC Leader tag.

### append_notes.sh — AB Notes Append
- **Type:** Scheduled (via Nightly ActionBuilder Update, step 4)
- **Civis script:** [#348368977](https://platform.civisanalytics.com/#/scripts/containers/348368977)
- **APIs:** ActionBuilder API (~4 req/sec, throttled 0.3s), BigQuery (read)
- **Input view:** `actionbuilder_sync.1mc_notes`
- **Description:** Appends 1MC conversation notes to entities (Event Host Notes, Conversation Host Notes, Event Attendee Notes). Idempotent via sync_log (keyed on airtable_record_id + response_name).

### connect_entities.sh — AB Organizing Team Connect
- **Type:** Scheduled (via Nightly ActionBuilder Update, step 5)
- **Civis script:** _(create container script; add to workflow #119217 after AB Notes Append)_
- **APIs:** ActionBuilder API (~4 req/sec, throttled 0.3s), BigQuery (read + sync_log write)
- **Input view:** `actionbuilder_sync.organizing_team_connects`
- **Description:** Connects existing AB entities (OFP training attendees in a state campaign) to the crosscutting **Organizing Team** campaign (id 26) and stamps their universal `Trainings > Organizing For Power` competencies, via `update_entity_with_tags` (POST person.identifiers → connect + add tags in one call). Idempotent via sync_log `connect_entity` rows (skips already-connected entities, covering BQ replication lag).

### insert_organizing_team.sh — AB Organizing Team Inserts
- **Type:** Scheduled (via Nightly ActionBuilder Update, step 6)
- **Civis script:** _(create container script; add to workflow #119217 after AB Organizing Team Connect)_
- **APIs:** ActionBuilder API (~4 req/sec, throttled 0.3s), BigQuery (read + sync_log write)
- **Input view:** `actionbuilder_sync.organizing_team_inserts`
- **Description:** Inserts OFP attendees who are not in AB and have no state-load path (no resolvable staffed state) directly into the Organizing Team campaign (id 26), with only the universal OFP competencies set. Insert guards: first_name NOT NULL, gmail plus-alias filter; excludes anyone already in AB or routed to a state campaign by `deduplicated_names_to_load`.

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
