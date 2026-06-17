# Organizing Team Campaign — Build Plan

**Status:** planned, not started · **Last updated:** 2026-06-12
**Campaign:** "Organizing Team", id **26**, UUID `1e7e58fd-efb4-4810-91dc-2e7aac08625a` (currently a sandbox)

## Purpose

A crosscutting (non-state) campaign for the organizing team to call OFP training attendees
and recruit them into Million Conversations involvement. Members span multiple states,
including unstaffed states whose people exist only in this campaign. Scope is deliberately
simple: load the OFP universe, populate basic OFP/involvement fields, and let organizers
set **1MC Prospect status** (a universal field) during phone banks. That prospect status is
the only human-entered field — **the sync never writes it** (the staged `1mc_prospects`
feed stays unwired from `TAG_COLS`).

## Verified API semantics (live tests, 2026-06-11)

Test driver: `scripts/test_connect_semantics.py`. Test subjects left in place: Ariyah Sadler
(`956a041d-…`, NC + Organizing Team) and Devon Bhakta (`cfafa185-…`, Test + Organizing Team).

1. **Connect vs duplicate.** Person Signup Helper POST *without* `identifiers` matches
   within the target campaign only → creates a duplicate entity. POST *with*
   `person.identifiers = ['action_builder:<interact_id>']` matches the network-level entity
   and **connects it to the target campaign** (same interact_id, new `campaigns_entities`
   row). Baseline: before this work, all 53,334 entities were single-campaign; every
   cross-campaign person in AB history is a duplicate entity.
2. **Universal sections** (set only at section creation; currently 1 Million Conversations
   and Election Protection) are single network-level tag objects — same tag interact_ids in
   every campaign, auto-present in new campaigns. Campaign 26 was born with the 9 1MC tags
   + 1 EP tag and nothing else.
3. **Universal value sharing works per-entity.** A tagging written via one campaign is
   immediately visible via any campaign the entity belongs to. Campaign-local sections
   (Participation, Activism, Engagement) never cross over. Duplicate entities therefore
   silently opt out of shared status — connect-by-interact-id discipline matters in every
   load path.
4. **Replace-on-write.** Writing a universal number field via any campaign replaces the
   existing tagging (new tagging id, one value network-wide). No accumulation.
5. **⚠️ Universal taggings are API-undeletable.** `/campaigns/{cid}/tags/{tag}/taggings`
   404s ("Tag is not accessible") for universal tags — list and DELETE both — and the
   signup helper's `remove_tags` returns 200 but silently no-ops. Only "removal" is
   replacement. Two consequences: universal fields must take a **no-removal sync path**
   (our 404-tolerant `delete_tagging` would log the failure as success), and universal
   statuses must be designed to progress/replace, never clear. Clearing requires the UI.

## Load universe

Anyone with ≥1 attended OFP training (Mobilize event 907019 timeslots via the
`ofp_training_map` seed). Sized 2026-06-12: **496 people**.

| Segment | Count | Path |
|---|---|---|
| Already in AB, one state-campaign entity | 354 | Connect by interact_id |
| In AB, state entity + Test dupe | 3 | Connect the state entity (Test excluded from match pool) |
| In AB, multiple state entities | 9 | Pick-one rule, then connect |
| In AB, **Test entity only** | 4 | Review bucket → one-time manual PA insert |
| Not in AB | 135 | Staffed states: wait for regular sync (structural lag); unstaffed/NULL state: insert into campaign 26 |

Context on the Test dupes: Test was populated with a legacy load of PA names during system
setup — duplicate entities of PA-campaign people, no shared interact_ids. The 4 Test-only
people are deadlocked (connect feed won't link sandbox records; the regular sync's
network-wide AB-exclusion won't insert them into PA) — hence manual fix.

**The lag is structural, not scheduled:** a staffed-state attendee not yet in AB appears in
*neither* feed. The regular sync inserts them into their state campaign, BQ replication
catches up (1–2 nights), and then they surface in the connect feed. Only unstaffed-state
people take the direct-insert path.

## Architecture decision: parallel updates view

`updates_needed` and the existing flow stay as-is, **plus an exclusion of campaign 26**.
A new lightweight `organizing_team_updates` view serves campaign 26. Rationale:

- Zero regression risk to the nightly 24-campaign sync.
- The exclusion structurally enforces single-owner universal writes: 1MC fields are only
  ever written through a state campaign's row, and dual-campaign row fan-out can't occur
  in the main flow.
- **Test stays in the main view** — the `test_campaign_updates` preview harness depends on
  it. The organizing flow validates via `--dry-run` plus the sandbox-connected entities.
- The parallel view is composition, not new logic: `ofp_attendance` is already
  entity+campaign keyed, so `WHERE campaign_id = 26` gets the OFP branch for free
  (`updates_needed` takes the complement); involvement-number branches reuse
  `correct_participation_values` diffed against `current_tag_values WHERE campaign_id = 26`.
  Same wide `_tag`/`_tag_remove` output format → `update_records` just needs a source-view
  parameter.
- Removal regimes stay separated by the view boundary: campaign-local fields in 26 keep
  normal delete-then-post; universal 1MC fields are no-removal.
- **Future note:** people who exist only in campaign 26 (unstaffed states) can only receive
  universal 1MC values (Total Conversations etc.) through the organizing view. When 1MC
  tags get wired into `TAG_COLS`, add those branches to `organizing_team_updates`, scoped
  to entities with no state-campaign membership.

## Phases

### Phase 0 — Decisions + UI prereqs (Rob)
- [ ] Create campaign-local fields in the campaign 26 UI **with the same names as the
      state campaigns** — minimum `Activism > Organizing for Power` (Organizing Basics,
      Storytelling, Relational Organizing, Rapid Response Basics). Same-names is what lets
      `ofp_attendance` extend automatically.
- [ ] Decide which other involvement fields to include (Events Attended Past 6 Months,
      Online Actions, …) and create them in the UI.
- [ ] NULL/unknown-state rule for the insert feed (recommend: insert into campaign 26).
- [ ] Multi-entity pick-one rule (recommend: home-state campaign's entity, else most
      recently updated).
- [ ] Confirm `1mc_prospects` stays unwired (prospect status is phone-bank-set).
- [ ] Delete Devon Bhakta's leftover universal tagging "Total Conversations = 2" in the UI
      (API cannot — doubles as a check that UI deletion works for universal tags).

### Phase 1 — Dual-campaign safety audit (before any mass connect)
Connecting entities breaks the standing single-campaign invariant. Audit, using Ariyah,
Devon, and the Test/PA twins as live fixtures:
- [ ] `updates_needed` — campaign scoping of current-vs-correct comparisons; add the
      campaign 26 exclusion; universal-field (1MC) branches must emit one row per entity
      and never emit removal strings.
- [ ] `current_tag_values` (+ `_bq_only`) — how a universal tagging (single row, one
      `campaign_id`) reads when diffing the other campaign.
- [ ] `dedup_candidates` / `dedup_ambiguous` — dual-campaign entities must not look like
      duplicates.
- [ ] `apply_assessments` — exclude campaign 26 (no auto-assessments there).
- [ ] `hot_prospects`, `test_campaign_updates` — sanity-check entity-campaign joins.
- [ ] BQ verification once replication catches up: Ariyah/Devon `campaigns_entities` rows;
      which `campaign_id` the universal tagging `8cf537fa-…` carries in `taggable_logbook`.

### Phase 2 — dbt models
- [ ] `ofp_universe` — one row per person with ≥1 attended OFP training, with
      name/email/phone/state from Mobilize for the insert path.
- [ ] `organizing_team_connects` — universe ∩ AB entities (email join, verified/user_added;
      **candidate pool = state campaigns only**, Test and 26 excluded), pick-one rule,
      minus entities already in campaign 26 (BQ `campaigns_entities` + sync_log
      `connect_entity` overlay). Output: entity interact_id.
- [ ] `organizing_team_inserts` — universe minus AB matches, unstaffed/NULL states only,
      reusing the insert guards (test-account filter, gmail canonicalization,
      `first_name IS NOT NULL`, sync_log inserted filter). Output:
      `campaign_interact_id` = campaign 26.
- [ ] `organizing_team_review` — anything that doesn't classify cleanly (Test-only matches,
      ambiguous multi-entity cases) surfaces here instead of being silently mis-routed.
- [ ] `organizing_team_updates` — the parallel updates view (see above).

### Phase 3 — sync.py
- [ ] New `connect_entities` operation: reads `organizing_team_connects`, POSTs
      `person.identifiers` (+ `add_tags` to stamp OFP competencies at connect time),
      logs `connect_entity` + per-tag `add_tagging` to sync_log, supports
      `--dry-run/--limit/--delay`. Uses existing `ab.insert_entity` — **no
      ccef-connections change, no release-pin bump**.
- [ ] Parameterize the insert path (source-view option or thin wrapper) so the campaign-26
      feed reuses person-building + `INSERT_TAG_FIELDS`.
- [ ] Parameterize `update_records` by source view for `organizing_team_updates`.
- [ ] Add "Organizing Team" to `CAMPAIGN_ALIASES` (4 files per the new-campaign checklist).

### Phase 4 — Deploy, seed load, nightly wiring
- [ ] `bash dbt.sh run`; spot-check feed counts and **phone fill-rate** (calling program).
- [ ] Dry-runs; staged initial load — connects first, then inserts (~500 people ≈ minutes
      at ~4 calls/sec).
- [ ] One-time manual PA insert for the 4 Test-only people (connect feed then picks them
      up automatically).
- [ ] Nightly workflow #119217: add steps 5 (`connect_entities`) and 6 (campaign-26
      inserts/updates) after `append_notes`; new `civis/` shell scripts;
      `SCHEDULED_SCRIPTS.md`.
- [ ] Docs per the new-campaign checklist (README, `docs/sync_overview.md`,
      `.claude/CLAUDE.md`), noting campaign 26 is **not** a state campaign and must stay
      out of state routing.

## Known limitations (accepted)

- **Hard-delete gap is self-correcting here:** organizer removals from campaign 26 never
  replicate to BQ, so BQ keeps showing membership and the feeds never re-add someone an
  organizer deliberately removed.
- **Universal statuses can't be cleared via API** — design them to progress or be
  replaced; clearing is a UI action.
- Test/PA duplicate twins will accumulate independent (diverging) universal taggings once
  1MC tags flow — duplicate entities opt out of shared status by nature.

## References

- `scripts/test_connect_semantics.py` — Test A driver (connect vs duplicate)
- `docs/sync_overview.md` — pipeline architecture, "Adding a New Sync Job"
- `docs/actionbuilder_person_signup.md` — signup helper / identifiers reference
- Memory: `organizing_team_campaign.md` (verified semantics), `organizing_team_build_plan.md`
