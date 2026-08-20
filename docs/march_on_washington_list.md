# March on Washington call list (2026-08-20)

A 150-person phone-recruitment list for the March on Washington, built for Carlos
Childs (DMV organizer: DC / MD / VA / PA) and pushed into the **Organizing Team**
campaign (26).

This is the first **non-OFP** population in campaign 26. Everything below either
reuses existing machinery or generalises it; nothing about the OFP feeds changed.

## What Carlos sees

In campaign 26, the 150 carry the tag **`Engagement > Recruitment List > March on
Washington`** and each is connected to Carlos with the existing "Regional Organizer"
connection. Either one finds the list: filter campaign 26 by the tag, or open his
Connections tab (the tag is what separates these 150 from his ~50 OFP recruits).

## Selection

Ranking lives in `models/march_on_washington_targets.sql`. Pool (1,440 people):

- most recent AB address geocodes **within 40 miles** of the National Mall
  (38.8895, -77.0353) — Frederick / Annapolis / Leesburg / Manassas in, Baltimore out
- address state in **DC / MD / VA**
- **callable**: at least one phone with status `verified`/`user_added` normalising to
  10 digits, and *no* phone marked `do_not_contact` or `bad` (any such phone
  disqualifies the whole entity — a second good number does not restore consent)
- member of an active non-Test campaign; not suppressed; not an organizer

Ranked **"proven show-ups first"** — for a physical march, having actually turned out
before beats online volume, so the two are tiered rather than summed:

| tier | meaning | pool |
|---|---|---|
| 1 | attended a Mobilize event in the last 365 days | 230 |
| 2 | attended a Mobilize event ever | 339 |
| 3 | online engagement only | 957 |

Within tier: most recent attendance, then online action volume, then lifetime events.

Tier 1 alone holds 230 people, so **the 150-person list never reaches tier 2** — every
person on it attended a Common Cause event between **2026-02-26 and 2026-08-16**.
The split fell out of the ranking rather than being imposed: **DC 19 / MD 85 / VA 46**,
averaging 14.3 miles from the Mall and 12.9 online actions.

Attendance uses `mobilize_cleaned.cln_mobilize__participations` with `attended = True`.
Do **not** substitute `events_attended_past_6_months` — that metric never filters
`attended` and runs ~3.3x real attendance (it is an RSVP count).

## The list is frozen

`march_on_washington_targets` **re-ranks on every query** (recency of last event moves
nightly). The list that was actually written to AB is the frozen snapshot table
`actionbuilder_sync.march_on_washington_list` (150 rows, `frozen_at 2026-08-20
16:21 UTC`), built once with `CREATE TABLE AS SELECT ... WHERE target_rank <= 150`.
It sits **outside dbt** on purpose — same pattern as `suppression_list` — so a
`dbt run` cannot rebuild it and drift the membership out from under the AB writes.

To build a *new* list later, re-rank and freeze to a new table; do not overwrite this one.

## AB taxonomy created

| object | id | notes |
|---|---|---|
| field (tag_category) `Recruitment List` | 45 | section `Engagement` (group 9), **multiselect**, `append`, campaign-local |
| value (tag) `March on Washington` | 131 | Standard; tagging interact_id `bac496d2-e074-4757-8b8a-8b03c63511a3` |

Created via GraphQL `createTagCategory` / `createTag`, then activated in campaigns
**26, 22 (MD), 24 (VA), 25 (DC)** with `associateTagToCampaign` (per-value — this is
what prevents the 200-and-silently-drop failure) plus `addTagCategoryToCampaign`
(per-field — UI visibility). Activation was **verified by read-back** per campaign,
against an unactivated control campaign, not by trusting the mutation's 200.

Deliberately **campaign-local, not universal**: universal taggings are API-undeletable,
and this list should be clearable after the march. Multiselect + `append` so the field
can carry future recruitment lists alongside this one.

The field is **not** read by `current_tag_values` / `updates_needed`, so the nightly
pipeline will never remove or overwrite it. Clearing it after the march is a
deliberate manual act.

## Execution (2026-08-20)

| step | command | result |
|---|---|---|
| canary | `sync.py connect_entities --source march_on_washington_connects --limit 1` | ok=1, tagging read back from the AB API |
| rest | `sync.py connect_entities --source march_on_washington_connects --delay 0.3` | ok=149 err=0, 149 tags |
| assign | `sync.py assign_organizers --organizer Carlos --delay 0.3` | ok=141 err=0 skipped=10 |

Final state: **150/150** connected to campaign 26, tagged, and connected to Carlos.
`phantom_tag_writes` = 0. An 8-entity random sample was re-read from the AB API: 8/8
carried the tag.

The 141 (not 150) assignments are correct — 9 of the list were already in campaign 26
as OFP attendees and **already connected to Carlos**, so `organizing_team_assignments`
correctly anti-joined them out. Zero unexplained.

## Code changes

- **`models/march_on_washington_targets.sql`** (new) — the ranking.
- **`models/march_on_washington_connects.sql`** (new) — connect+tag feed off the frozen
  table. Unlike `organizing_team_connects` it does *not* filter out entities already in
  campaign 26: this is a one-shot list, the 14 already-members still need the tag, and a
  connect for an already-connected entity is a harmless no-op. Idempotency instead comes
  from a `sync_log` anti-join on the marker tag, so re-running never double-writes.
- **`scripts/sync.py`** — `connect_entities` gained `--source VIEW`, defaulting to
  `organizing_team_connects`. Any override must emit `campaign_interact_id`,
  `entity_interact_id`, `sync_string`.
- **`models/organizing_team_assignments.sql`** — state resolution is now
  `ofp_universe` (zip-derived) **then AB address state** as a fallback. Required
  because these 150 have no `ofp_universe` row at all. Existing OFP members are
  unaffected: `ofp_state` is collapsed with the same `ORDER BY state` tiebreak
  `routed` already applied. Side effect, measured before shipping: **10 existing
  campaign-26 members** whose Mobilize record carried no zip become routable and will
  be assigned on the next nightly (Lamair 6, Luana 3, Rommel 1).

## Gotchas hit

- `sync_log.tag_interact_id` is NULL on these 150 `add_tagging` rows. `_get_tag_map`
  reads tag ids from BQ, and tag 131 had not replicated yet at write time. Cosmetic —
  the write itself keys on the sync string, and the taggings are confirmed in AB.
- GraphQL rejects the whole document on an unknown selection field, so a bad selection
  set creates nothing. The first `createTagCategory` attempt selected a non-existent
  `errors` field and was a no-op; the retry checked for a pre-existing category before
  creating, to be sure the failed attempt had not half-applied.
- On `TagCategory`, `tags` is a plain list (no `first:`/`nodes`), and `Tag` has no
  `status` field in GraphQL.
