# Scheduled Scripts — ActionBuilder Sync

*Last verified: 2026-07-07*

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

**Clone timing:** each container script clones the repo at the start of *that
script's own run*, not at workflow start. So a push to `master` mid-workflow is
picked up by any step that hasn't started yet (and by every step on the next run).

## Workflows

### Nightly ActionBuilder Update
- **Civis name:** Nightly ActionBuilder Update — [workflow #119217](https://platform.civisanalytics.com/spa/#/workflows/119217)
- **Schedule:** Daily at 10:00 PM ET
- **Typical runtime:** 1.5–4 hours (observed)
- **Steps:** run_dbt.sh → insert_new_records.sh → update_records.sh → apply_assessments.sh → append_notes.sh → connect_entities.sh → insert_organizing_team.sh → assign_organizers.sh
- Step names in Civis: AB run_dbt / AB Inserts / AB Tag Updates / AB Assessment Setting / AB Notes Append / AB Organizing Team Connect / AB Organizing Team Inserts / AB Organizing Team Assign Organizers (sequential)
- **Definition:** exported to `civis/civis_workflow.yaml` (authoritative task graph — job ids + transitions). Transitions: run_dbt→Inserts→Updates use `on-success` (halt on failure); Assessments onward use `on-complete` (proceed regardless).

## Scripts

### run_dbt.sh — AB run_dbt (nightly step 0)
- **Type:** Scheduled (via Nightly ActionBuilder Update, step 0 — first step)
- **Civis script:** [#358132951](https://platform.civisanalytics.com/spa/#/scripts/containers/358132951) — wired as the first step of workflow #119217.
- **APIs:** BigQuery (recreates views; recomputes any table-materialized models)
- **Description:** Runs `dbt run` to refresh all `actionbuilder_sync` models so the downstream sync ops read current data. Installs `dbt-bigquery==1.11.0`; `run_dbt.py` loads `BIGQUERY_CREDENTIALS_PASSWORD` (Civis job env var) into a temp keyfile for the dbt profile. **Must be first** once any model is `materialized='table'` — a table is only as fresh as the last dbt run. Until then it's a harmless view refresh. Exits non-zero on dbt failure so the workflow halts rather than syncing stale data.

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
- **Description:** Sets engagement assessment levels automatically (upgrade-only). Level 1: Mobilize attendance, NewMode submission, any STW call, 20+ AN actions in 6mo, or 1MC Host tag. Level 2: 2+ STW calls, 2+ virtual Mobilize, any in-person CC Mobilize event, or hosted a 1MC event. Level 3: 1MC Leader tag. Full criteria: `docs/assessment_rules.md`.

### append_notes.sh — AB Notes Append
- **Type:** Scheduled (via Nightly ActionBuilder Update, step 4)
- **Civis script:** [#348368977](https://platform.civisanalytics.com/#/scripts/containers/348368977)
- **APIs:** ActionBuilder API (~4 req/sec, throttled 0.3s), BigQuery (read)
- **Input view:** `actionbuilder_sync.1mc_notes`
- **Description:** Appends 1MC conversation notes to entities (Event Host Notes, Conversation Host Notes, Event Attendee Notes). Idempotent via sync_log (keyed on airtable_record_id + response_name).

### connect_entities.sh — AB Organizing Team Connect
- **Type:** Scheduled (via Nightly ActionBuilder Update, step 5)
- **Civis script:** [#357827345](https://platform.civisanalytics.com/spa/#/scripts/containers/357827345) — created 2026-06-16 (cloned from AB Notes Append), added to workflow #119217 as step 5
- **APIs:** ActionBuilder API (~4 req/sec, throttled 0.3s), BigQuery (read + sync_log write)
- **Input view:** `actionbuilder_sync.organizing_team_connects`
- **Description:** Connects existing AB entities (OFP training attendees in a state campaign) to the crosscutting **Organizing Team** campaign (id 26) and stamps their universal `Trainings > Organizing For Power` competencies, via `update_entity_with_tags` (POST person.identifiers → connect + add tags in one call). Idempotent via sync_log `connect_entity` rows (skips already-connected entities, covering BQ replication lag).

### insert_organizing_team.sh — AB Organizing Team Inserts
- **Type:** Scheduled (via Nightly ActionBuilder Update, step 6)
- **Civis script:** [#357827433](https://platform.civisanalytics.com/spa/#/scripts/containers/357827433) — created 2026-06-16 (cloned from AB Notes Append), added to workflow #119217 as step 6
- **APIs:** ActionBuilder API (~4 req/sec, throttled 0.3s), BigQuery (read + sync_log write)
- **Input view:** `actionbuilder_sync.organizing_team_inserts`
- **Description:** Inserts OFP attendees who are not in AB and have no state-load path directly into the Organizing Team campaign (id 26), with only the universal OFP competencies set. "No state-load path" = neither zip-derived nor voter-file state is a staffed campaign (direct check). Insert guards: first_name NOT NULL, gmail plus-alias filter; excludes anyone already in AB. **Note (2026-06-18):** previously anti-joined `deduplicated_names_to_load`, which inlined the whole `master_load_qualifiers` tree and exceeded BigQuery's query planner — the step errored silently every run (0 inserts). Replaced with the direct staffed-state check. **Note (2026-07-29):** the "already in AB" check used only the BQ mirror, whose ~1-day lag meant night-N inserts still looked new on night N+1 — the 2026-06-20/21 runs double-inserted all 70 night-1 people (70 duplicate entity pairs in campaign 26). Fixed with a sync_log `already_inserted` overlay (same pattern as `deduplicated_names_to_load`); the 70 newer twins were removed from campaign 26 by the one-shot `remove_ot_duplicates` op (2026-07-29, ok=70 err=0 — feed `ot_duplicate_removal`, manual, NOT in nightly). The 17 other campaign-26 pairs (16 launch-window 6/17–18 + 1 legacy 2025) were triaged and removed the same day via a vetted one-shot (33 campaign removals — losing twins removed from both 26 and their state campaign; legacy pair from 26 only; keepers per `dedup_candidates`; logged as `remove_ot_duplicate`).

### assign_organizers.sh — AB Organizing Team Assign Organizers
- **Type:** Scheduled (via Nightly ActionBuilder Update, step 7 — terminal)
- **Civis script:** [#360156836](https://platform.civisanalytics.com/spa/#/scripts/containers/360156836) — created 2026-07-03, added to workflow #119217 as step 7 (after AB Organizing Team Inserts).
- **APIs:** ActionBuilder API (~4 req/sec, throttled 0.3s), BigQuery (read + sync_log write)
- **Input view:** `actionbuilder_sync.organizing_team_assignments`
- **Description:** Wires each Organizing Team (campaign 26) member to the C&O National Organizing Team organizer who covers their zip-derived state, as a People:People AB connection tagged "Regional Organizer" (section `Organizer Relationships`, field `Assigned Organizer`), via `ab.create_connection`. The connection *is* the assignment; the tag just labels it. Idempotent via sync_log `create_connection` rows (skips already-assigned members, covering BQ replication lag). The `organizer_state_map` seed covers every state/territory the zip crosswalk can produce: the 24 staffed states across the four regional organizers, plus (2026-07-29) all unstaffed states + territories routed to Tiffany Rubio (organizing intern). Only no-zip-state members are left unassigned.
- **Note:** all staged-rollout `--organizer` holds are gone (last removed 2026-07-29) — the job runs unfiltered for all five organizers. History: Lamair Bryan's hold lifted 2026-07-29 (account activated; 141 assigned); Tiffany Rubio's same-day hold lifted after the `remove_ot_duplicates` cleanup removed the 70 double-insert twins from campaign 26 and her 71 clean members were assigned. Prereq for any future organizer: both ends of a connection must be members of campaign 26 — connect via `update_entity_with_tags(26, interact_id, [])`, NOT the AB UI (the UI add was observed not to persist, 2026-07-02).

## On-Demand Scripts

### snapshot_tag_state.sh
- **Type:** On-demand (not in nightly workflow)
- **APIs:** ActionBuilder API (~4 req/sec, throttled 0.3s), BigQuery (write)
- **Description:** Captures current tag state from AB API for all campaigns and logs `add_tagging` rows to `actionbuilder_sync.sync_log`. Used for recovery/healing sync_log gaps — critical to run before retrying `update_records` after a failed sync.

### remove_suppressed (no shell script yet — run locally)
- **Type:** On-demand (added 2026-07-30)
- **APIs:** ActionBuilder API (~4 req/sec, throttled 0.3s), BigQuery (read + sync_log write)
- **Input view:** `actionbuilder_sync.suppression_removal`
- **Description:** The **suppression layer**: a curated do-not-sync list (`actionbuilder_sync.suppression_list`, BQ table managed outside dbt — add entries with `scripts/add_suppression.py`, one row per email / person_id / entity interact_id). This op removes suppressed people's entities from ALL active campaigns (logs `remove_suppressed`, listed in `removed_campaign_entities` so the feed self-clears); the insert/connect feeds (`deduplicated_names_to_load`, `organizing_team_inserts`, `organizing_team_connects`) permanently exclude listed identifiers so the sync never re-adds them. Run after adding entries: `python scripts/sync.py remove_suppressed --dry-run`, then live. First entry 2026-07-30 (per Rob).

### cleanup_duplicate_tags.sh
- **Type:** On-demand (not in nightly workflow)
- **APIs:** ActionBuilder API (~4 req/sec, throttled 0.3s)
- **Description:** Removes duplicate tag values from entities across all campaigns. Calls DELETE on individual tag-entity associations. Originally a post-dedup operation; available for periodic use.

### remove_ep_externals.sh
- **Type:** On-demand (one-shot; executed 2026-06-18, not scheduled)
- **APIs:** ActionBuilder API (~4 req/sec, throttled 0.3s), BigQuery (read + sync_log write)
- **Input view:** `actionbuilder_sync.ep_external_removal`
- **Description:** Removes partner-org EP volunteers who were loaded via the old EP-shift path (anti-poaching cleanup). Executed 2026-06-18 (188 removed); the feed now reads 0 via the `removed_campaign_entities` overlay. The script body defaults to `--dry-run` with the live line commented out; its header says to archive it after execution.

### remove_duplicate_entities.sh
- **Type:** On-demand supervised pass (executed March 2026 and 2026-07-29; not scheduled)
- **APIs:** ActionBuilder API (~4 req/sec, throttled 0.3s), BigQuery (read)
- **Description:** Three-phase dedup: migrate secondary emails to keeper entities (`prepare_email_data`), migrate phone numbers (`prepare_phone_data`), then remove duplicate entities (`remove_records` over `dedup_candidates`). March 2026: 374 resolved. **2026-07-29: 535 resolved** (356 emails + 163 phones migrated first; the big blocks were VA-launch double-inserts from 2026-04-09/10 and Michigan voterbase matches). Feed is now empty except 33 `test_account` rows with no campaign (unremovable by this op).
- **Cadence:** run supervised roughly quarterly, or after any bulk-load event (new campaign launch, new load qualifier). Before running, check: no organizer/staff entities in the feed, no row where `delete_tag_count > keep_tag_count`, and inspect any row whose keeper is in `removed_campaign_entities` (2026-07-29: those were lingering twins of purged externals — removing them was correct). Full runbook: `docs/deduplication.md` § Deletion Workflow.
