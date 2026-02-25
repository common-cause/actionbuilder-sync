# ActionBuilder Sync — Project Overview

## What This Project Does

This project calculates participation values for people in ActionBuilder from external platforms (Mobilize, Action Network, ScaleToWin, EP Archive) and pushes those values into ActionBuilder as tag responses via a custom sync job. It runs entirely in BigQuery as a set of dbt views.

**Current status:** `update_records` fully operational — test campaign synced successfully (Feb 2026). Dedup execution is the next step before Wisconsin campaign full run.

---

## The Sync Job

The sync job is `scripts/sync.py` in this repository. It replaces the original TMC consultant script, which was a proprietary wrapper around the [Parsons ActionBuilder library](https://github.com/move-coop/parsons/blob/main/parsons/action_builder/action_builder.py). Our script uses the `ActionBuilderConnector` from `ccef-connections` and makes direct OSDI API calls.

- **Input:** BigQuery views in `actionbuilder_sync` dataset
- **Authentication:** `ACTION_BUILDER_CREDENTIALS_PASSWORD` env var — JSON `{"api_token": "...", "subdomain": "..."}`
- **Rate limit:** AB API is approximately 4 calls/second; no batch endpoint
- **CLI:** `python scripts/sync.py <operation> [--campaign UUID] [--dry-run] [--limit N]`

### Operations

| Operation | BQ input view | Purpose |
|---|---|---|
| `update_records` | `updates_needed` | Update tag values on existing entities — **operational** |
| `insert_new_records` | `deduplicated_names_to_load` | Create new entities — built, not yet active |
| `remove_records` | `dedup_candidates` | Hard-delete duplicate entities via `DELETE /people/{id}` |
| `prepare_email_data` | `email_migration_needed` | Add secondary emails to keeper entities before dedup deletion |
| `prepare_phone_data` | `phone_migration_needed` | Add secondary phones to keeper entities before dedup deletion |

### Output Table Format (`updates_needed`)

The sync reads from `actionbuilder_sync.updates_needed`. Required structure:

| Column pattern | Purpose | Format |
|---|---|---|
| `entity_id` | ActionBuilder entity to update | 36-char UUID interact_id (e.g. `abc123de-...`) — NOT the short numeric id |
| `*_tag` | Add this tag value | Full sync string (see below) |
| `*_tag_remove` | Remove this existing tag value | `tag-interact-id:|:tagging-interact-id` |

**Rules:**
- Column names before `_tag` / `_tag_remove` are arbitrary — name them for human readability
- Multiple rows per entity are fine and equivalent to one row with JSON arrays
- Multiple values for the same field in one row: use JSON array notation — `["value1","value2"]`
- Blank values after the last `:|:` in a `_tag` column will cause errors — never send an empty value

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
New record insertion infrastructure. Identifies people from external platforms who qualify to be added to ActionBuilder but don't yet have a record. `deduplicated_names_to_load` applies full AB exclusion and within-feed dedup. **Not currently active** — blocked on dedup execution. As of Feb 2026: 36,044 genuinely new records ready to insert.

#### `test_campaign_updates`
Filtered view of `updates_needed` for the Test campaign only, with first/last name and primary email joined in for easy human identification. Use to verify what `update_records` will do on the test campaign before running live.

#### `test_campaign_update_summary`
Aggregated breakdown of pending test campaign updates by field and change type, with min/max/avg-delta for numeric fields. Use as a quick sanity-check dashboard.

---

## Currently Synced Fields

| Field Name | Section | Category | Type | Source |
|---|---|---|---|---|
| Events Attended Past 6 Months | Participation | Event Attendance Summary | number | Mobilize |
| Most Recent Event Attended | Participation | Event Attendance History | date | Mobilize |
| First Event Attended | Participation | Event Attendance History | date | Mobilize |
| Action Network Actions | Participation | Online Actions Past 6 Months | number | Action Network |
| Action Network State Actions | Participation | Online Actions Past 6 Months | number | State Action Network |
| Top State Action Taker | Participation | State Online Actions | standard | State Action Network |
| Phone Bank Calls Made | Participation | Event Attendance Summary | number | ScaleToWin |

---

## Known Issues and Open Problems

### 1. Tag removal — IMPLEMENTED AND OPERATIONAL ✓
`current_tag_values` exposes `removal_string = CONCAT(tag_interact_id, ':|:', taggable_logbook_interact_id)`. The `updates_needed` view passes these into `*_tag_remove` columns. The sync script performs a separate DELETE call per tagging (404-tolerant) before writing the new value. Update Value rows include removal IDs; New Value rows have NULL. The sync replaces values rather than accumulating them.

### 2. Duplicate AB entities — SCRIPT OPERATIONAL, EXECUTION READY

**Scale (Feb 2026, verified against live BQ):**
- 23,116 total entities; 99.8% matched to a voter-file `person_id`
- 475 `person_id`s map to 2+ AB entities
- `dedup_candidates` view identifies **584 entities to delete** across 5 tiers

**Root cause:** Pipeline run multiple times, each run creates new AB records rather than matching to existing ones. See [docs/deduplication.md](deduplication.md) for full analysis.

**Current state — all views built, script operational:**
- `email_migration_needed` — 302 emails to migrate to keeper entities (`prepare_email_data`)
- `phone_migration_needed` — 183 phones to migrate to keeper entities (`prepare_phone_data`)
- `dedup_candidates` — 584 entities to delete (`remove_records`)
- 1 unresolved dedup pair (Catherine Turcer) blocks `deduplicated_names_to_load`

**Execution order:**
1. `email_migration_needed` → sync (`prepare_email_data`)
2. `phone_migration_needed` → sync (`prepare_phone_data`)
3. `dedup_candidates` → sync (`remove_records`)
4. Resolve Catherine Turcer pair → `bash dbt.sh run -s dedup_candidates deduplicated_names_to_load`
5. `deduplicated_names_to_load` → sync (`insert_new_records`)

**Why `identity_resolution` has `person_id` commented out:**
Was commented out due to the duplicate problem. Can be restored once dedup is complete.

### 3. New record insertion — BUILT AND GUARDED, NOT YET ACTIVE

`deduplicated_names_to_load` is the insertion feed. As of Feb 2026: **35,926 rows**, all genuinely new.

**Guards in place:**
- AB exclusion by person_id (covers all identity-hub-linked emails including migrated secondaries)
- AB exclusion by direct email match
- AB exclusion by phone (phone-only records)
- Test account filter (gmail plus-aliases)
- Within-feed dedup: gmail canonical normalization (Pass A) + name+phone matching (Pass B)

Activation blocked on executing the dedup sequence above first.

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

## Adding a New Sync Field

To add a new participation field to the sync:

1. **Add the source data** — create or extend a staging view with the raw data
2. **Add to `correct_participation_values`** — join in the new data, format the value, build the sync string using the pattern `Section:|:Category:|:Field:|:response_type:value`
3. **Add to `current_tag_values`** — add the tag name to the `WHERE tag_name IN (...)` filter and add a CASE branch in `sync_field_identifier`
4. **Add to `updates_needed`**:
   - Add the correct/current value columns to `value_comparisons`
   - Add the `*_needs_update` boolean
   - Add a `UNION ALL` branch in `updates_to_apply`
   - Add `*_tag` and `*_tag_remove` CASE branches in the final SELECT
5. **Run `bash dbt.sh run`** to deploy

## Adding a New Sync Job (new campaign / different fields)

The architecture supports multiple independent sync jobs targeting different campaigns or field sets. Each would be a separate view following the same `*_tag` / `*_tag_remove` output format. Consider creating a subdirectory under `models/` per sync job if the number of views grows significantly.
