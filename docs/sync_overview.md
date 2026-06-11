# ActionBuilder Sync — Project Overview

## What This Project Does

This project calculates participation values for people in ActionBuilder from external platforms (Mobilize, Action Network, ScaleToWin, EP Archive) and pushes those values into ActionBuilder as tag responses via a custom sync job. It runs entirely in BigQuery as a set of dbt views.

**Current status (2026-06-11):** Nightly maintenance runs on Civis at 10 PM ET (workflow #119217): `insert_new_records` → `update_records` → `apply_assessments` → `append_notes` across all 24 state campaigns (22 original + VA + DC). Dedup completed March 2026. Platforms feeding tags: Mobilize, Action Network (incl. state actions), ScaleToWin, NewMode, EP Archive, OFP training. 1 Million Conversations (1MC) role/conversation/notes models are rolling out (see `MEMORY.md` → 1MC roadmap).

---

## The Sync Job

The sync job is `scripts/sync.py` in this repository. It replaces the original TMC consultant script, which was a proprietary wrapper around the [Parsons ActionBuilder library](https://github.com/move-coop/parsons/blob/main/parsons/action_builder/action_builder.py). Our script uses the `ActionBuilderConnector` from `ccef-connections` and makes direct OSDI API calls.

- **Input:** BigQuery views in `actionbuilder_sync` dataset
- **Authentication:** `ACTION_BUILDER_CREDENTIALS_PASSWORD` env var — JSON `{"api_token": "...", "subdomain": "..."}`
- **Rate limit:** AB API is approximately 4 calls/second; no batch endpoint
- **CLI:** `python scripts/sync.py <operation> [--campaign NAME_OR_UUID] [--dry-run] [--limit N] [--delay SECONDS]`

### Operations

| Operation | BQ input view | Purpose |
|---|---|---|
| `update_records` | `updates_needed` | Update tag values on existing entities — **nightly** |
| `insert_new_records` | `deduplicated_names_to_load` | Create new entities — **nightly** |
| `apply_assessments` | `auto_assessment_rules` | Set assessment levels (upgrade-only) — **nightly** |
| `append_notes` | `1mc_notes` | Append 1MC conversation notes to entities — **nightly** |
| `snapshot_tag_state` | (API-driven) | Capture tag ground truth from AB API into sync_log — **on-demand** |
| `remove_records` | `dedup_candidates` | Remove duplicate entities from campaigns — **one-time, completed** |
| `prepare_email_data` | `email_migration_needed` | Migrate emails to keeper entities before dedup — **one-time, completed** |
| `prepare_phone_data` | `phone_migration_needed` | Migrate phones to keeper entities before dedup — **one-time, completed** |

### Output Table Format (`updates_needed`)

The sync reads from `actionbuilder_sync.updates_needed`. Required structure:

| Column pattern | Purpose | Format |
|---|---|---|
| `entity_id` | ActionBuilder entity to update | 36-char UUID interact_id (e.g. `abc123de-...`) — NOT the short numeric id |
| `*_tag` | Add this tag value | Full sync string (see below) |
| `*_tag_remove` | Remove this existing tag value | `tag-interact-id:|:tagging-interact-id` |

**Rules:**
- The `_tag` / `_tag_remove` column names are **NOT arbitrary** — they are a closed set. `sync.py` iterates a hardcoded `TAG_COLS` list (and `REMOVE_COLS = [c + '_remove' ...]`), so `update_records` only processes columns in that list. Any `_tag` column `updates_needed` emits that is *not* in `TAG_COLS` is silently ignored. The two must stay in sync. See **"How a tag reaches the column"** below.
- Multiple rows per entity are fine and equivalent to one row with JSON arrays
- Multiple values for the same field in one row: use JSON array notation — `["value1","value2"]`
- Blank values after the last `:|:` in a `_tag` column will cause errors — never send an empty value

#### How a tag reaches the column (`field_group` → output column → `TAG_COLS`)

This is the part most easily misread from the SQL. In `updates_needed`, each field's `updates_to_apply` row carries a `field_group` string. The final SELECT uses `field_group` in `CASE` expressions to route that row's `sync_string` into exactly **one** named output column (and the removal IDs into its `*_remove` partner). The output column — not the field name — is what `sync.py` reads.

`TAG_COLS` in `scripts/sync.py` (the live set as of 2026-06):

```
event_participation_history_tag      online_actions_past_6_months_tag    ofp_tag
event_participation_summary_tag      state_online_actions_tag
national_online_actions_tag          engagement_tag
```

Consequences when adding a tag:
- **Reuse an existing column** (give your row a `field_group` that maps to one already in `TAG_COLS`) → **no `sync.py` change needed.** This is the **NewMode precedent**: "NewMode Actions" uses `field_group = 'Online Actions Past 6 Months'` and rides the existing `online_actions_past_6_months_tag` column. The actual AB tag taxonomy is set by the sync string, not the column name, so reuse is clean.
- **New column** → you must add it to **both** the `updates_needed` final SELECT (a new `field_group` → `_tag`/`_tag_remove` CASE pair) **and** `TAG_COLS` in `sync.py`.
- The columns differ in removal behavior: most do add-with-removal (update replaces value); the `ofp_tag` and the 1MC columns are additive-only (removal always NULL). Pick a column whose behavior matches your tag.
- ⚠️ The 1MC `million_conversations_*_tag` columns are emitted by `updates_needed` but are **not yet in `TAG_COLS`** — they are staged ahead of the `sync.py` wiring (1MC rollout in progress). They are not written by `update_records` today.

### Sync String Format

Tag values use a 4-part `:|:`-delimited string:

```
Section:|:Category:|:Field:|:response_type:value
```

Examples:
```
Participation:|:Event Attendance Summary:|:Events Attended Past 6 Months:|:number_response:12
Participation:|:Event Attendance History:|:First Event Attended:|:date_response:2024-03-15
Participation:|:State Online Actions:|:Top State Action Taker:|:standard_response:Top State Action Taker
```

**Response types** (the "fourth layer"):
- `number_response:` — numeric values
- `date_response:` — dates in `YYYY-MM-DD` format
- `standard_response:` — text/standard tags
- `address_response:` — postal addresses (not currently used)

Non-standard tag types (anything other than Standard) always require the fourth layer. Standard tags technically don't, but using `standard_response:` is safe.

### Tag Removal Format

To remove an existing tag value before setting a new one, `current_tag_values` exposes a `removal_string`:

```
tag-interact-id-from-tags-table:|:tagging-interact-id-from-taggable-logbook
```

Both IDs are 36-char UUIDs from ActionBuilder's own database:
- First ID: `interact_id` from `actionbuilder_cleaned.cln_actionbuilder__tags` for that specific field
- Second ID: `interact_id` from `actionbuilder_cleaned.cln_actionbuilder__taggable_logbook` for that specific person+field tagging

**How removal works in the sync script (`sync.py`):**

Tag removal is a two-step process — NOT a `remove_tags` parameter in the POST body (that parameter does not exist in the AB API and causes a 500 error):

1. **Step 1 — DELETE existing tagging:** `DELETE /campaigns/{campaign_id}/tags/{tag_id}/taggings/{tagging_id}`. A 404 response is treated as success — if the tagging no longer exists (e.g. previously cleared by another sync run), the desired state is already achieved.
2. **Step 2 — POST new value:** `POST /campaigns/{campaign_id}/people` via the Person Signup Helper with `add_tags`. For "Clear Value" rows (correct value is 0/empty), step 2 is skipped.

**Data staleness note:** The tagging IDs in `current_tag_values` come from our BQ snapshot of AB data. If another sync has run since the last BQ pull, those tagging IDs may no longer exist in the live AB system. The 404-tolerant delete handles this gracefully.

---

## Running the Sync

```bash
# Recurring update (all active campaigns):
python scripts/sync.py update_records

# Recurring update (one campaign only):
python scripts/sync.py update_records --campaign 0e41ca37-e05d-499c-943b-9d08dc8725b0

# Test with first N rows before full run:
python scripts/sync.py update_records --campaign <uuid> --limit 10

# Dry run — logs what would happen, no API calls:
python scripts/sync.py update_records --dry-run

# Dedup execution order (run once):
python scripts/sync.py prepare_email_data --campaign <uuid>
python scripts/sync.py prepare_phone_data --campaign <uuid>
python scripts/sync.py remove_records --campaign <uuid>
python scripts/sync.py insert_new_records --campaign <uuid>
```

**Campaigns:**
- Test: `0e41ca37-e05d-499c-943b-9d08dc8725b0` (552 entities — use for validation)
- Wisconsin: `12951a1f-...` (look up via BQ: `SELECT interact_id FROM actionbuilder_cleaned.cln_actionbuilder__campaigns WHERE name LIKE '%Wisconsin%'`)

**Live test results (2026-02-23):**
- Test campaign `update_records`: 146 entity groups, ok=146 err=0, 150 tags written

---

## The Data Pipeline

```
External Platforms          Staging Views           AB Sync Views          Sync Job
──────────────────          ─────────────           ─────────────          ────────
Mobilize               ──► mobilize_event_data ──┐
Action Network         ──► action_network_actions  │
                       ──► action_network_6mo      ├──► correct_participation_values ──┐
ScaleToWin             ──► scaletowin_call_data  ──┤                                   │
State Action Network   ──► state_an_top_performers ┘                                   ├──► updates_needed ──► sync job
                                                                                        │
ActionBuilder DB ──► cln_actionbuilder__* ──► current_tag_values ────────────────────┘
```

### Key Views

#### `correct_participation_values`
The "what should be in AB" view. Joins ActionBuilder entities to external platform data via ALL email addresses and phone numbers associated with each entity (not just primary). Aggregates across multiple emails using `SUM` for counts and `MIN`/`MAX` for dates. Outputs one row per entity with formatted values and complete sync strings.

Source datasets joined in: `core_enhanced.enh_activistpools__emails` and `_phones` serve as the linking hub between AB entities and external platforms.

#### `current_tag_values`
The "what is currently in AB" view. Reads from `cln_actionbuilder__taggable_logbook`, `cln_actionbuilder__tags`, and `cln_actionbuilder__global_notes`. Gets the most recent non-deleted tag application for each entity+tag combination. Returns one row per entity/tag.

Exposes `removal_string = CONCAT(tag_interact_id, ':|:', taggable_logbook_interact_id)` — both 36-char UUID interact_ids needed by the sync's `_tag_remove` columns. All 46,893 rows have this populated.

#### `updates_needed`
The sync job's input table. Compares correct vs. current values, filters to only records where something has changed, and outputs in the wide column format the sync expects. One row per entity per field update. Uses a correlated subquery to look up the 36-char `interact_id` from `cln_actionbuilder__entities`.

#### `identity_resolution`
Maps ActionBuilder entities to `person_id`s in the `core_enhanced` hub via primary email and phone. Currently has `person_id` commented out of the output due to the unresolved duplicate problem (see below). Used for diagnostic purposes.

#### `email_migration_needed` + `phone_migration_needed`
Pre-deletion contact migration feeds. For each duplicate pair in `dedup_candidates`, identifies emails/phones on the entity to be deleted that are not yet on the keeper entity. Must be run (via `prepare_email_data` / `prepare_phone_data`) before executing deletions to ensure participation data is preserved.

#### `master_load_qualifiers` + `deduplicated_names_to_load`
New record insertion infrastructure. Identifies people from external platforms who qualify to be added to ActionBuilder but don't yet have a record. `deduplicated_names_to_load` applies full AB exclusion and within-feed dedup. Runs nightly via `insert_new_records`. Filters out records with NULL first_name (AB API requires it).

#### `test_campaign_updates`
Filtered view of `updates_needed` for the Test campaign only, with first/last name and primary email joined in for easy human identification. Use to verify what `update_records` will do on the test campaign before running live.

#### `test_campaign_update_summary`
Aggregated breakdown of pending test campaign updates by field and change type, with min/max/avg-delta for numeric fields. Use as a quick sanity-check dashboard.

---

## Currently Synced Fields

Section is `Participation` for every field except Hot Prospect (`Engagement`). The last column is the `TAG_COLS` output column in `updates_needed` / `sync.py` (see "How a tag reaches the column").

| Field Name (AB field) | Category (field_group) | Type | Source model | → TAG_COLS column |
|---|---|---|---|---|
| Events Attended Past 6 Months | Event Attendance Summary | number | `mobilize_event_data` | `event_participation_summary_tag` |
| Phone Bank Calls Made | Event Attendance Summary | number | `scaletowin_call_data` | `event_participation_summary_tag` |
| Most Recent Event Attended | Event Attendance History | date | `mobilize_event_data` | `event_participation_history_tag` |
| First Event Attended | Event Attendance History | date | `mobilize_event_data` | `event_participation_history_tag` |
| Action Network Actions | Online Actions Past 6 Months | number | `action_network_6mo_actions` | `online_actions_past_6_months_tag` |
| Action Network State Actions | Online Actions Past 6 Months | number | `state_action_network_top_performers` | `online_actions_past_6_months_tag` |
| NewMode Actions | Online Actions Past 6 Months | number | `newmode_actions` | `online_actions_past_6_months_tag` |
| Top State Action Taker | State Online Actions | standard | `state_action_network_top_performers` | `state_online_actions_tag` |
| Top National Action Network Activist | National Online Actions | standard | `action_network_national_top_performers` | `national_online_actions_tag` |
| Hot Prospect *(Section: Engagement / Cat: Prospect Identification)* | — | standard | `hot_prospects` (Mobilize+AN+STW+NewMode activity) | `engagement_tag` |
| OFP competencies: Organizing Basics, **Storytelling**, Relational Organizing, Rapid Response Basics | Organizing for Power | standard (additive multi-select) | `ofp_attendance` (Mobilize event 907019) | `ofp_tag` |

**Notes:**
- **`current_tag_values` is an overlay model:** it merges `sync_log` tag operations on top of the (sometimes stale) BQ snapshot of `taggable_logbook`, so the "what is currently in AB" side reflects recent sync runs and covers the hard-delete replication gap. A `_bq_only` twin preserves the original snapshot-only logic. See `CLAUDE.md` → "Sync Log Architecture".
- **Name-collision warning:** "**Storytelling**" already exists as an OFP training competency tag (above). A Soapboxx storytelling tag must use a distinct name (e.g. "Soapboxx Stories").
- **1MC (in progress):** `updates_needed` also emits `million_conversations_*` columns (roles, total conversations, prospects) from the `1mc_*` models, and `append_notes` writes 1MC notes from `1mc_notes`. The tag columns are not yet in `sync.py` `TAG_COLS` (rollout pending).

---

## Known Issues and Open Problems

### 1. Tag removal — IMPLEMENTED AND OPERATIONAL ✓
`current_tag_values` exposes `removal_string = CONCAT(tag_interact_id, ':|:', taggable_logbook_interact_id)`. The `updates_needed` view passes these into `*_tag_remove` columns. The sync script performs a separate DELETE call per tagging (404-tolerant) before writing the new value. Update Value rows include removal IDs; New Value rows have NULL. The sync replaces values rather than accumulating them.

### 2. Duplicate AB entities — EXECUTED ✓

**Execution (March 2026):**
- 154 emails migrated, 91 phones migrated to keeper entities
- 8,921 entities removed from campaigns
- 3,532 new entities inserted
- 16 same-campaign ambiguous pairs remain in `dedup_unresolved` — not yet actioned

See [docs/deduplication.md](deduplication.md) for full analysis and root cause.

### 3. New record insertion — OPERATIONAL ✓

`deduplicated_names_to_load` runs nightly via `insert_new_records`. Guards in place:
- AB exclusion by person_id, direct email, and phone
- Test account filter (gmail plus-aliases)
- Within-feed dedup: gmail canonical normalization + name+phone matching
- Sync_log filter prevents re-inserting entities already logged as inserted
- `WHERE first_name IS NOT NULL` filter prevents AB API 422 errors on nameless records

---

## BigQuery Datasets Used

| Dataset | Contents |
|---|---|
| `actionbuilder_sync` | This project's views (managed by this dbt project) |
| `actionbuilder_cleaned` | Cleaned/normalized ActionBuilder DB tables (`cln_actionbuilder__*`) |
| `core_enhanced` | Cross-platform identity hub (`enh_activistpools__emails`, `_phones`) |
| `mobilize_cleaned` | Cleaned Mobilize participation data |
| `actionnetwork_cleaned` | Cleaned Action Network user and action data |
| `scaletowin_dialer_cleaned` | Cleaned ScaleToWin call data |
| `ep_archive` | EP internal shift data |
| `actionnetwork_views` | Reference tables (e.g. `states`) |
| `targetsmart_enhanced` | Voter file data (used as fallback in new record load) |

---

## Incorporating a New Participation Platform

This is the canonical recipe for wiring a new activist platform (Mobilize, AN, ScaleToWin, NewMode, Soapboxx, …) into the sync. A platform is incorporated across **three surfaces**, mirroring how the existing ones work:

1. **Record involvement** — its activity becomes an AB participation tag (the steps below).
2. **Push criterion** — its activity qualifies people to be *loaded* into AB (`master_load_qualifiers`).
3. **Hot prospects** — its activity counts toward the prospect ranking (`hot_prospects`).

You usually want all three. They are independent edits, so build and validate each in isolation.

### Prerequisites
- **The AB tag field must already exist in the AB UI** and be added to the campaign before the API can write it. The Person Signup Helper matches `section` + `field` + `name` to existing tags (unless `action_builder:create_tag` is set — which this sync does not use). Create the section/field in the UI first. (See `docs/actionbuilder_person_signup.md`; and `MEMORY.md` → `ab_notes_field_type` for the notes-field gotcha.)
- **Identity keys:** the platform's per-person data must carry email and/or phone — that's how it joins to AB entities (`cln_actionbuilder__emails`/`_phones`) and to the `core_enhanced` person hub. Confirm fill rates before building.
- **Pick a distinct tag name** — check the "Currently Synced Fields" table for collisions (e.g. "Storytelling" is taken by OFP).

### Surface 1 — Record involvement (the tag)
1. **Source model** — create a staging model that dedups the platform's cleaned BQ table to one row per person (per email), e.g. `soapboxx_stories.sql` modeled on `action_network_6mo_actions.sql` / `newmode_actions.sql`. Emit the metric value(s) and the `Section:|:Category:|:Field:|:response_type:value` sync string(s).
2. **`correct_participation_values`** — join the new data in (via the entity's emails/phones), format the value, expose the value + sync-string columns; include it in `has_participation_data`.
3. **`current_tag_values`** (and `_bq_only`) — add the new tag name(s) to the `tag_name IN (...)` read list so current AB values are diffed and old values can be removed.
4. **`updates_needed`** — add the tag to the current-values pivot, `value_comparisons`, the `*_needs_update` boolean, a `UNION ALL` branch in `updates_to_apply`, and route it via a `field_group`:
   - **Reuse an existing `TAG_COLS` column** by choosing a matching `field_group` (no `sync.py` change — the NewMode pattern), **or**
   - **Add a new column** to the final SELECT *and* to `TAG_COLS` in `scripts/sync.py`.
   See "How a tag reaches the column" above. The actual AB taxonomy is set by the sync string, so reusing a column does not constrain the tag's real section/field.
5. *(Optional — tag on first insert)* — to stamp the value when a brand-new entity is created (not just on later updates), add a value column to `deduplicated_names_to_load` and an entry to `INSERT_TAG_FIELDS` in `sync.py`.

### Surface 2 — Push criterion (`master_load_qualifiers`)
- Add a `<platform>_qualifiers` CTE (one row per qualifying person with name/email/phone/created_at and a `qualification_reason` literal), add it to the `all_qualifiers` UNION, and — if the activity is genuine CC engagement — add it to `cc_engaged_emails` (the anti-poaching override). Choose the threshold: high-effort actions qualify at ≥1 (Mobilize/NewMode); high-volume online actions use a per-state threshold (AN). Downstream `deduplicated_names_to_load` then handles dedup/exclusion automatically.

### Surface 3 — Hot prospects (`hot_prospects`)
- Add a `<platform>_activity` CTE (count per entity via the entity-email join), LEFT JOIN it in `entity_activity`, add it to `total_activity_score` (flat 1-per-action keeps the score comparable across platforms), and add an output column.

### Deploy & verify
- `bash dbt.sh run -s <new_model>+` to build the model and its downstream dependents.
- Spot-check counts in BigQuery; use `test_campaign_updates` / `test_campaign_update_summary` to preview what `update_records` will write before a live run.
- Dry-run the sync: `python scripts/sync.py update_records --campaign <test-uuid> --dry-run`.

## Adding a New Sync Job (new campaign / different fields)

The architecture supports multiple independent sync jobs targeting different campaigns or field sets. Each would be a separate view following the same `*_tag` / `*_tag_remove` output format. Consider creating a subdirectory under `models/` per sync job if the number of views grows significantly.
