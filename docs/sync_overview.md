# ActionBuilder Sync — Project Overview

## What This Project Does

This project calculates participation values for people in ActionBuilder from external platforms (Mobilize, Action Network, ScaleToWin, EP Archive) and pushes those values into ActionBuilder as tag responses via a custom sync job. It runs entirely in BigQuery as a set of dbt views.

**Current status:** Updating tag values on existing records only. New record insertion is built but not yet active — `dedup_candidates` view is ready; AB API deletions not yet executed.

---

## The Sync Job

The sync job is a custom script written by a Movement Cooperative consultant. It reads a BigQuery view and makes API calls to ActionBuilder. Key characteristics:

- **Input:** A BigQuery table/view with one row per entity (or per field update) in a specific wide-column format
- **Authentication:** ActionBuilder OSDI API token
- **Rate limit:** 4 calls/second, no batch endpoint
- **No public documentation** — behavior documented here from consultant emails

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

To remove an existing tag value before setting a new one:

```
tag-interact-id-from-tags-table:|:tagging-interact-id-from-taggable-logbook
```

Both IDs are 36-char UUIDs from ActionBuilder's own database:
- First ID: `interact_id` from `actionbuilder_cleaned.cln_actionbuilder__tags` for that specific field
- Second ID: `interact_id` from `actionbuilder_cleaned.cln_actionbuilder__taggable_logbook` for that specific person+field tagging

**Current status:** Tag removal is fully implemented. `current_tag_values` exposes `removal_string` for every existing tag application, and `updates_needed` passes these into `*_tag_remove` columns. The sync now replaces values rather than accumulating them.

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

#### `master_load_qualifiers` + `deduplicated_names_to_load`
Infrastructure for new record insertion. Identifies people from external platforms who qualify to be added to ActionBuilder but don't yet have a record. **Not currently active.** Qualification sources: EP shifts (2024), Mobilize events (past year), ScaleToWin phone bank, Action Network (20+ actions in 6 months).

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

### 1. Tag removal — IMPLEMENTED ✓
`current_tag_values` exposes `removal_string = CONCAT(tag_interact_id, ':|:', taggable_logbook_interact_id)`. The `updates_needed` view passes these into `*_tag_remove` columns. Update Value rows include the removal IDs; New Value rows have NULL. The sync replaces values rather than accumulating them.

### 2. Duplicate AB entities — `dedup_candidates` VIEW BUILT, DELETIONS PENDING

**Scale (Feb 2026, verified against live BQ):**
- 23,116 total entities; 99.8% matched to a voter-file `person_id`
- 475 `person_id`s map to 2+ AB entities
- `dedup_candidates` view identifies **374 entities to delete** (341 via person_id match, 33 test accounts)

**Root cause:** Pipeline run multiple times, each run creates new AB records rather than matching to existing ones. Sept 8, 2025 (13,453 entities), June 3, 2025 (5,354), Apr 18, 2025 (550) are the major import dates; each created hundreds of duplicates.

**Current state:**
- `models/dedup_candidates.sql` is deployed and tested
- Deletions via the ActionBuilder API have NOT yet been executed
- See [docs/deduplication.md](deduplication.md) for the full workflow

**Why `identity_resolution` has `person_id` commented out:**
Was commented out due to the duplicate problem. Can be restored once dedup is complete.

### 3. Only updating existing records
New record creation is turned off. The path to turning it on is:
1. Solve or accept the duplicate problem
2. The `deduplicated_names_to_load` view is already built as the candidate new-record input
3. The sync job presumably supports a creation mode — needs confirmation of the exact table format expected

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
