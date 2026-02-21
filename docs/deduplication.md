# Deduplication — Strategy and Workflow

## Background

All ActionBuilder entities were created by the sync pipeline — nothing was hand-entered. This means any entity can be safely deleted without losing human work. The problem is that the pipeline has been run multiple times, and each run that created new entities did so without checking whether that person already existed in AB, resulting in duplicate records.

---

## Scale of the Problem (Feb 2026 baseline)

| Metric | Count |
|--------|-------|
| Total AB entities | 23,116 |
| Entities matched to a voter-file `person_id` | 23,080 (99.8%) |
| `person_id`s with 2+ AB entities | 475 |
| Total entities to delete via `dedup_candidates` | **374** |
| — of which: person_id_match tier | 341 |
| — of which: test_account tier | 33 |
| Entities with zero tags (unsynced) | ~998 |

---

## Root Cause

Every time the sync pipeline has been run to create new records, it submits all qualifying people to ActionBuilder without checking if they already exist there. The batch import dates and the duplicates they created:

| Import Date | Entities Created | Same-day person_id collisions |
|-------------|-----------------|-------------------------------|
| 2025-09-08 | 13,453 | ~2,146 within the batch |
| 2025-06-03 | 5,354 | ~763 |
| 2025-04-18 | 550 | ~309 |
| 2025-08-30 | 905 | ~178 |

The Sept 8 batch was so fast that thousands of consecutive entity-creation gaps were under 100ms — multiple workers were submitting the same records concurrently.

Cross-batch duplicates (same person imported on two different dates) account for most of the remaining cases, particularly the Apr 18 → Jun 3 re-import pattern.

---

## How `dedup_candidates` Works

The view (`models/dedup_candidates.sql`) outputs one row per entity to delete, with a pointer to the canonical entity to keep.

### Tier 1: `person_id_match`

Every AB entity's primary email is looked up in `core_enhanced.enh_activistpools__emails` to get a `person_id`. Two entities that map to the same `person_id` are definitively the same person.

**Keep rule:** Within each `person_id` group, keep the entity with the most tags (tags = participation data written by the sync). Tiebreak: keep the oldest entity (`created_at ASC`). Mark all others for deletion.

This tier handles groups of 2, 3, 4, 5, 6, or more entities correctly.

### Tier 2: `name_email_match`

Fallback for entities whose email doesn't match anything in `core_enhanced` (only ~36 entities as of Feb 2026). Same dedup logic but grouping by (first_name + last_name + email_norm) instead of person_id.

### Tier 3: `test_account`

Gmail plus-alias accounts (email matching `^[^+]+\+[^@]+@gmail\.com$`) are test accounts created during pipeline development. Examples: `izzy.bronstein+13@gmail.com`, `izzy.bronstein+ccg@gmail.com`. These are deleted outright — `keep_interact_id` is NULL for this tier.

---

## Notable Groups

These are the highest-impact duplicate groups, confirmed in the research data:

| Name | Entities | Notes |
|------|----------|-------|
| Antione Fields | 6 | Staff — 1 new entity created per import batch since June 2025 |
| Joshua Valdez | 6 | Staff with multiple email addresses |
| Isabella Bronstein | 8 | Staff — 7 gmail+ test aliases + 1 real `@commoncause.org` account |
| Cheech Sorilla | 7 | Staff with multiple emails |
| Brenda Davies | 3 | Batch-millisecond duplicate (Sept 8 concurrent worker bug) |
| Dorothy Johnson | 3 | Includes a `@gmal.com` (typo) record from April batch |

---

## Deletion Workflow

### Step 1: Review the view output

```sql
SELECT * FROM `proj-tmc-mem-com.actionbuilder_sync.dedup_candidates`
ORDER BY group_size DESC, delete_last_name
LIMIT 50;
```

Spot-check: confirm `keep_interact_id` points to the right record and `delete_interact_id` points to the obvious duplicate.

### Step 2: Delete via ActionBuilder API

Use the AB OSDI API to delete each entity in `delete_interact_id`. The API endpoint is:

```
DELETE /api/v1/campaigns/{campaign_id}/people/{interact_id}
```

A script needs to be written to iterate over `dedup_candidates` and call the API for each `delete_interact_id`. **Do not delete entities where `keep_interact_id` is NULL without confirming they are test accounts** — spot check a few rows first.

### Step 3: Verify sync recovery

After deletion, run `bash dbt.sh run` to refresh all views, then confirm that `updates_needed` no longer references the deleted entities. The `current_tag_values` view reads from live AB data, so it will naturally drop the deleted records.

### Step 4: Fix the pipeline

Before running any future batch inserts (`deduplicated_names_to_load`), add a guard to `master_load_qualifiers` or `deduplicated_names_to_load` that excludes anyone whose email or person_id already exists in AB. This prevents the next import from recreating duplicates.

### Step 5: Enable new record insertion

Once dedup is clean and the guard is in place, enable new record insertion. As of Feb 2026:
- `deduplicated_names_to_load` has ~50,564 rows
- ~36,662 are genuinely new (not yet in AB by email)
- ~13,963 already exist in AB (will be filtered by the guard)

---

## Address Data

ActionBuilder already has address records for virtually all entities, stored in `actionbuilder_cleaned.cln_actionbuilder__addresses` (linked by `owner_id = entity.id`, `owner_type = 'Entity'`). Coverage:
- State: 100%
- Postal code: 99.9%
- City: 99%
- Street address: 82%

No need to pull addresses from source systems for dedup purposes — AB already has them.

---

## Edge Cases Not Automated

These require human judgment and are not in `dedup_candidates`:

- **Same email, different last names** (~3-4 cases): likely shared family emails (e.g. `pedptz@gmail.com` → Jonathan + Leanne Paetz). Keep both unless confirmed same person.
- **"Winston Laura" vs "Laura Winston"**: clear name-entry reversal error. Person is already in the person_id tier.
- **Same name, all different contact info** (~729 name pairs): could be genuine name-alikes. Skip automated dedup; handle manually if needed.
- **Julie Berberi (8 entities, all Apple Hide My Email addresses)**: created in a 600ms burst — batch bug, but the icloud.com random aliases aren't in core_enhanced so they have no person_id. All have the same name; can be merged manually.
