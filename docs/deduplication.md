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

## Contact Migration — Before Deletion

Before deleting duplicate entities, their email addresses and phone numbers must be migrated to the keeper entities. This ensures:

1. **Participation data is preserved** — `correct_participation_values` joins directly from `cln_actionbuilder__emails` and `cln_actionbuilder__phone_numbers` to external platform data. Once a delete entity's emails/phones are on the keeper entity, all activity associated with those contacts is correctly attributed to the keeper.
2. **Future dedup prevention** — the keeper entity's expanded contact set ensures the identity-hub-based exclusion in `deduplicated_names_to_load` correctly blocks re-creation of these people.

### `email_migration_needed` (Feb 2026: 91 emails → 87 keeper entities)

Collects all emails from each entity in `dedup_candidates` (not just the primary — every verified/user_added email), excludes any already present on the keeper entity, and outputs one row per email to transfer.

Run via sync script: `prepare_email_data` operation.

### `phone_migration_needed` (Feb 2026: 93 phones → 89 keeper entities)

Identical logic for phone numbers. Only includes valid 10-digit numbers.

Run via sync script: `prepare_phone_data` operation.

---

## Deletion Workflow

### Step 1: Review the view output

```sql
SELECT * FROM `proj-tmc-mem-com.actionbuilder_sync.dedup_candidates`
ORDER BY group_size DESC, delete_last_name
LIMIT 50;
```

Spot-check: confirm `keep_interact_id` points to the right record and `delete_interact_id` points to the obvious duplicate. Pay special attention to rows where `keep_interact_id IS NULL` (test_account tier — delete with no redirect).

### Step 2: Migrate contact info

Run `email_migration_needed` through the sync script using `prepare_email_data`, then `phone_migration_needed` using `prepare_phone_data`. Confirm a sample of keeper entities now show the additional emails/phones before proceeding.

### Step 3: Delete via sync script

Run `dedup_candidates` through the sync script using `remove_records`.

> **Note:** The TMC sync script's `remove_records` operation may remove entities from campaign membership rather than hard-deleting them. Confirm with the consultant whether this achieves a true deletion or just deactivation. If only deactivation, the AB OSDI API `DELETE /api/v1/campaigns/{campaign_id}/people/{interact_id}` endpoint may be needed instead.

### Step 4: Verify sync recovery

Run `bash dbt.sh run` to refresh all views, then confirm `updates_needed` no longer references the deleted entities. The `current_tag_values` view reads from live AB data, so it will naturally drop deleted records.

### Step 5: Enable new record insertion

`deduplicated_names_to_load` is ready and fully guarded. As of Feb 2026 (post-migration counts):

- **35,926 rows** — genuinely new people not in AB
- Filtered by: person_id (covers all identity-hub-linked emails including migrated secondaries), direct email match, phone-only phone match
- Further deduplicated within the feed: gmail canonical normalization (Pass A) collapsed email variants; name+phone matching (Pass B) collapsed 195 additional records where the same person appeared under different email addresses

Run via sync script: `insert_new_records` operation.

---

## How `deduplicated_names_to_load` Prevents Future Duplicates

After the dedup execution, re-running the sync will not recreate duplicates because:

1. **Person_id exclusion** — for anyone with a `core_enhanced` person_id, we check whether that person_id is linked to any current AB entity (via the identity hub across all emails, including newly migrated secondaries). If so, excluded.

2. **Direct email exclusion** — for unmatched records (no person_id), their email must not already appear in `cln_actionbuilder__emails`.

3. **Phone-only exclusion** — for records with no email, their phone must not already appear in `cln_actionbuilder__phone_numbers`.

4. **Test account exclusion** — gmail plus-aliases are filtered from the incoming feed.

5. **Within-feed dedup** — gmail canonical normalization and name+phone matching prevent a single person from generating multiple new-record rows.

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

---

## July 2026 Execution (2026-07-29)

Second full pass, prompted by the campaign-26 duplicate discovery (70 `insert_organizing_team`
double-inserts + 17 launch-window/legacy pairs — see `civis/SCHEDULED_SCRIPTS.md` notes).
The feed had accumulated **568 rows** since March: 338 voterbase_id_match, 195 person_id_match,
35 test_account. The largest blocks were **Virginia (134 person_id rows)** — the VA campaign
launch re-running the replication-lag double-insert on 2026-04-09/10 — and Michigan (93
voterbase rows).

Pre-run integrity checks (all passed or explained):
- 0 self-pairs, 0 duplicate delete targets, 0 rows touching organizer/staff entities,
  0 rows deleting a richer twin than the keeper.
- 1 A→B→C chain (three entities, one person): both removals ran; terminal keeper survives.
- 3 rows whose keeper was already removed by the EP/Mobilize external cleanups: their delete
  twins were lingering entities of deliberately purged externals — removal completes that cleanup.
- 42 "keeper not in campaign" rows = 33 test_account delete-only rows (no keeper) + 9 by-design
  cross-campaign keepers.

Execution: `prepare_email_data` (356, ok=356) → `prepare_phone_data` (163, ok=163) → keeper
spot-check via live API → `remove_records` (**ok=535, err=0**, 33 no-campaign test rows skipped).
Feed residue: the 33 test_account rows (no campaign to remove from — permanently unremovable
by this op). `dedup_unresolved` manual-review queue: 170 pairs, untouched.

Prevention shipped the same day: `insert_entity` sync_log rows now record the inserted email
(`value_written`), and both insert feeds exclude by logged email as well as person_id — one
email carrying multiple core person_ids was the mechanism behind the VA-launch and June
launch-window duplicates. The `dedup_candidates` wrapper was also repointed at
`removed_campaign_entities` so removals by any cleanup op clear the feed.

**Cadence:** re-run this workflow (steps 1–4 above) supervised, roughly quarterly or after any
bulk-load event (new campaign, new load qualifier).

---

## July 2026 — dedup_unresolved cleared by agent review (2026-07-30)

All **170 pairs** in `dedup_unresolved` were resolved in one agent (Claude) review session,
using evidence beyond what `ai_resolve_dedup.py` feeds GPT-4o: street addresses, all
secondary emails/phones, campaign memberships, and activity recency per entity. Decisions
written to `dedup_resolutions` (`resolved_by='ai:claude-fable-5'`): **118 MERGE / 52
KEEP_BOTH / 0 DEFER** (6 initial defers — couples sharing an email — resolved KEEP_BOTH per
Rob). 114 merge rows flowed into `dedup_candidates` (`resolved_merge` tier; the other 4 were
already-removed twins or trio-duplicates) and were executed by the standard supervised pass
the same day. Notable rule applications: corruption-over-tags (repeated/concatenated names
deleted even when richer — contact migration preserves their emails/phones and the nightly
sync recomputes participation tags on the keeper), a reversed-surname corruption
("Sioramed" = "Demarois" backwards), and household KEEP_BOTHs decided by shared street
address + distinct personal emails. The dominant lesson: **most ambiguity comes from couples
sharing an email address** — email is not an individual-level identifier in this membership.

Also added 2026-07-30: the **suppression layer** (`suppression_list` table +
`remove_suppressed` op + insert/connect feed guards) for people who must be removed and
never re-synced. See `civis/SCHEDULED_SCRIPTS.md`.
