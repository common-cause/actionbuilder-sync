# Proposal: A Redesigned Section & Field Taxonomy for ActionBuilder

**Status:** APPROVED by Rob 2026-08-13 — all recommendations accepted as written, with one modification: 1MC prospect statuses stay in the 1MC section rather than moving to Engagement (see D8, §8). Phase 0 executed 2026-08-07 (§2.1). **Phase 1 (experiments E1–E4) executed 2026-08-13 — all four PASS** (§3), removing every mechanical gate: Interests can be universal (E1), D8's single-select rotation works and the `1MC Prospect Status` field is live (E2), retirement is archive-only (E3), renames are safe (E4). A bonus discovery — per-campaign field enablement (§3 item 8) — adds a Phase 2 step and surfaced that the universal OFP field is currently invisible in every campaign. Next: Phase 2.
**Author:** Claude, at Rob's request, 2026-07-30.
**Scope:** The tag taxonomy (sections → fields → values) across all campaigns. Assessments, connections mechanics, and the sync pipeline architecture are out of scope except where the taxonomy touches them.

---

## 1. Why redesign, and why now

1. **The current taxonomy is accreted, not designed.** 12 person-tag sections exist. Four of them are effectively dead (zero or near-zero usage), several overlap ("Volunteer Activity Interest" vs "Expressed Volunteer Interest" vs "Level of Volunteer Activity Interest"), and there are three name collisions ("Organizing For Power" exists as two different fields; "Storytelling" is both an OFP competency and a Participation field; "Total Conversations" is both a field and a stray value under a different field).
2. **Campaign-local sections have a proven, silent failure mode.** Every campaign-local section must be hand-created in each campaign's UI. Virginia and DC never got theirs — see §2.1. Universal sections cannot fail this way: they auto-appear in every campaign, including future ones.
3. **Organizing Team ↔ state campaign work needs shared visibility.** Campaign-local taggings don't cross campaigns, so an organizer working campaign 26 can't see a member's event counts or online actions (those live in the state campaign's local Participation section). The OT program is exactly the cross-campaign collaboration the current design can't serve.
4. **Search UX pushes hard toward self-describing value names.** In the main search interface the **value name** is the most prominent thing; section and field are secondary. Today, finding EP 2024 shift workers means typing "**2024**" (the value under Election Protection > Shifted is literally named `2024`). Values like `Opt-In`, `RSVP Date`, `Meeting`, `1/1/2025` are meaningless out of context. Deliberately longer, self-locating value names fix this.
5. **The adoption window is open.** Few teams actively use AB yet; almost all populated fields are sync-owned, so we can restructure by repointing the sync rather than retraining users. Also well-timed: the 1MC tag columns are staged but **not yet wired into `TAG_COLS`** (ship them with the new names, never the old), the Soapboxx tag has zero taggings, and EP 2026 is ramping (get the year-qualified names in place *before* bulk 2026 tagging).

---

## 2. Current state (inventory as of 2026-07-30)

Source: `actionbuilder_cleaned` — `tag_groups` (= sections) → `tag_categories` (= fields) → `tags` (= values) → `taggable_logbook` (taggings). "Entities" = distinct entities with a live tagging (the reliable number; raw live-tagging counts are inflated by the hard-delete replication gap).

### Campaign-local sections (linked per campaign via `campaigns_tag_groups`)

| Section | Campaigns | Field | Values | Entities | Verdict |
|---|---|---|---|---|---|
| **Participation** | 23 (missing VA, DC) | Event Attendance History | First Event Attended, Most Recent Event Attended (+ stray "Event Attended") | 18,836 | sync-owned, keep (move) |
| | | Event Attendance Summary | Events Attended Past 6 Months; Phone Bank Calls Made | 9,595; 2,185 | sync-owned, keep (move) |
| | | Online Actions Past 6 Months | Action Network Actions; Action Network State Actions; NewMode Actions | 46,951; 6,675; 1,776 | sync-owned, keep (move) |
| | | National Online Actions | Top National Action Network Activist | 94 | sync-owned, keep (move) |
| | | State Online Actions | Top State Action Taker | 934 | sync-owned, keep (move) |
| | | Storytelling | Soapboxx Stories | 0 | sync-owned, keep (move) |
| | | Event Participation | "1/1/2025", "3/1/2025" | 0 | **dead — archive** |
| | | Event Participation Type | 29 imported enum values (ADVOCACY … WORKSHOP) | 0 | **dead — archive** |
| | | First Event Attendance | "1/1/2025" | 0 | **dead — archive** |
| | | Specific Events Attended | "2025-10-20 NM ABQ RCV Rally" | 0 | **dead — archive** |
| **Engagement** | 22 (missing VA, DC, Test) | Prospect Identification | Hot Prospect | 662 | sync-owned, keep & extend |
| **Activism** | 23 | Organizing For Power | 4 competencies (legacy campaign-local twin) | ~194 | superseded by Trainings — archive |
| | | Action Participation | Event/Meeting/Phonebank/Rally/Webinar (dates) | 2 | archive |
| | | Recent Activism | Within 6 Month / Within 1 year | 3 | archive (sync supersedes) |
| | | Volunteer Activity Interest | 8 values (Phone Banking, Poll Monitoring, …) | 19 | consolidate → Interests |
| **Issues** | 22 | Issue Bucket Interest | 3 values | 9 | consolidate → Interests |
| | | Issue Participation | (no values) | 0 | archive |
| **Expressed Volunteer Interest** | 13 | Volunteer Interest; Level of Volunteer Activity Interest; EP Swag Captain | 7 values total | 0 | consolidate → Interests |
| **Event RSVP Dates** | 11 | Inactives Phonebank RSVP Date | RSVP Date | 9 | archive (confirm w/ owner) |
| **Action Team** | PA only | Action Team Opt-In | Opt-In | 86 | consolidate → Local Groups |
| **Chapters and Subgroups** | NE only | Nebraska Regional Group | 5 values (3 seem to be the same District 8 group) | 2 | consolidate → Local Groups |

### Universal sections (network-level; taggings carry NULL campaign_id; auto-appear everywhere)

| Section | Field | Values | Entities | Verdict |
|---|---|---|---|---|
| **Election Protection** | Shifted | **"2024"** | 1,428 | keep section; rename field + values; make sync-owned |
| **Trainings** | Organizing For Power | Organizing Basics (325), Storytelling (251), Relational Organizing (239), Rapid Response Basics (13) | 828 | keep; rename values |
| **1 Million Conversations** | Million Conversations Role | Host, Leader, Participant | 0 | keep; rename values |
| | Total Conversations | Total Conversations (number) | 1 | keep; rename value |
| | Million Conversations Prospect | Host Prospect, Leader Prospect, stray "Total Conversations" | 0 | keep in 1MC (Rob 2026-08-13, D8); rotation via replace-on-write, see §6.2 + E2 |
| | Conversation Notes | Conversation Host Notes, Event Attendee Notes, Event Host Notes | ~10 | keep; rename values |

### Connection-type section (unchanged by this proposal)

| Section | Field | Value | Connections |
|---|---|---|---|
| **Organizer Relationships** (OT only) | Assigned Organizer | Regional Organizer | 493 |

### 2.1 ⚠️ Incidental finding: VA and DC have never received a single tag

Discovered while building this inventory — **independent of the proposal and worth acting on regardless.**

- Campaigns 24 (Virginia) and 25 (District of Columbia), created 2026-04-09, have **zero rows in `campaigns_tag_groups`** — the campaign-local sections (Participation, Engagement, …) were never created in their UIs. Not a replication artifact: `campaigns_entities` replicates fine for both (1,120 / 124 entities), and `taggable_logbook` has zero rows for them **including deleted** ones.
- Meanwhile `sync_log` shows **21,049 `add_tagging ok` to Virginia and 2,787 to DC** (2026-04-09 → 2026-07-13). The Person Signup Helper silently ignores invalid fields, so AB returned 200 and every one of those taggings was dropped on the floor.
- **The sync-log overlay masks the failure**: `current_tag_values` trusts the logged "ok" writes, so `updates_needed` believes the values are current and stopped re-emitting them. The `_bq_only` model would show them all as perpetually needed.
- Not everything is broken there: assessments landed (VA 1,111 / DC 119 — assessments aren't tags), campaign membership is fine, and universal-section writes (OFP, 1MC notes) land fine because universal fields exist everywhere.

Remediation options are folded into the migration plan (§7, Phase 0). The generalized lesson — *a campaign-local write can silently no-op and our own log will then hide it* — motivates both the universal-by-default design and the phantom-write detector (§6).

> **STATUS 2026-08-07 — Phase 0 EXECUTED (D6 resolved as "patch now"); the rest of this proposal remains a draft awaiting review.**
> The mechanics turned out to be *activation*, not creation: sections/fields/values are single network-level objects that campaigns activate per tag **value** (`campaigns_tags` junction; `AssociateTagToCampaign` mutation behind the Customize → Info → Edit Info checkboxes). Executed: all 11 sync-written values activated in VA/DC (dead fields skipped); ~23.8K phantom sync_log rows annotated `status='phantom'` (overlay re-emits); canary writes verified landing; `phantom_tag_writes` detector model shipped. The detector immediately found two more silent drops, both fixed same day: **Soapboxx Stories was activated in zero campaigns** (all 9 writes since June dropped; now active in all 24) and 36 pre-universal-cutover OFP writes to VA/DC (universal re-stamp already covered those entities). Note for §6.4/E3: this incidental finding doesn't resolve E1–E4 — the experiments still gate the redesign.

---

## 3. What we know about AB mechanics (constraints the design must respect)

Verified behaviors (live API tests 2026-06-11 unless noted):

1. **Universality is chosen section-by-section, only at section creation.** Existing campaign-local sections can't be flipped; new universal sections must be created fresh. Existing universal sections: Election Protection, 1 Million Conversations, Trainings.
2. **Universal tags are one network-level object** — same interact_id in every campaign, auto-present in new campaigns at creation. Zero per-campaign setup.
3. **Universal value sharing is per-entity** — visible across campaigns only for the *same* entity connected to multiple campaigns. (Historic cross-campaign people are duplicate entities; dedup + connect-based OT population is converging on shared entities.)
4. **Replace-on-write works for universal number tags** — POSTing a new number value replaces the existing tagging network-wide, no accumulation. So mutable counters *can* be universal.
5. **Universal taggings are API-undeletable** — DELETE/list 404, signup-helper `remove_tags` silently no-ops. Consequences:
   - "Clear Value" (delete, write nothing) is impossible → for universal number fields, **write an explicit 0 instead of clearing**.
   - Standard flags that must be *removed* when a person rotates off a list (Hot Prospect, Top 50, prospect flags) **cannot live in a universal section**.
6. **Invalid fields are silently ignored** on signup-helper writes (the VA/DC failure mode).
7. **Search UX:** the value name is what users see and type against; field/section are secondary.

### Experiments E1–E4 — ALL RUN 2026-08-13, all four PASS

- **(E1) UI-deletability of universal taggings: ✅ YES.** Profile → Info → tagging row menu → "Remove Info" works and **persists server-side** (verified via API listing after each delete) for every universal tag type tested: standard multiselect, standard single-select, and number. Devon Bhakta's leftover `Total Conversations = 2` cleaned up as the number-type test. So: humans can maintain universal fields (Interests can go universal, D3), and the API-undeletable constraint is **API-only** — the UI escape hatch exists.
- **(E2) Replace-on-write for standard universal tags: ✅ YES — but only for single-select fields.** Live-tested on Testy McTesterson (entity 1, Test campaign):
  - *Multiselect* standard fields (all pre-existing ones) **accumulate across values** — writing Leader after Host leaves both. Same-value re-write replaces itself (GraphQL `multiselectSameTagBehavior: "overwrite"` — no duplicates).
  - **Single-select is a field-creation option** ("Response options: Multi/Single select" radio, settable only at creation), and a single-select standard universal field **replaces on write across values** — Host Prospect → Leader Prospect → None, exactly one tagging at every step. **D8's rotation design is confirmed viable.** The production field `1MC Prospect Status` (category id 31, single-select, locked) now exists in the 1MC section with values `1MC Host Prospect` / `1MC Leader Prospect` / `1MC Prospect: None`. Consequence for §6.1: the new **Top Performers** Engagement field should also weigh single-select vs multiselect at creation (a person can hold both Top State and Top National → separate single-select fields or one multiselect with sync-managed removal, as drafted).
- **(E3) Archived values in search: ✅ HIDDEN.** The response-picker search hides archived values: the archived "Total Conversations" standard twin doesn't appear while its live number sibling does (identical enablement conditions); the archived `Rally` date value is hidden while the *active* dead `RALLY` enum still pollutes — proving both that archiving cleans search and that un-archived dead fields are worth archiving. **§6.4 cleanup = archive-only; no mass-delete of ~350K taggings needed.**
- **(E4) Rename preserves interact_id + taggings: ✅ YES.** UI-renamed `Host` → `1MC Host` in Million Conversations Role: tag object kept interact_id `8d8cd0ec-…`, and a pre-existing tagging survived, displaying the new name (verified via API tag GET + tagging listing). The full D4 rename list is mechanically safe. (This rename is done and kept — first of the Phase 2 batch; no code references 1MC names yet.)

### ⚠️ E-bonus discovery (2026-08-13): per-campaign FIELD enablement governs UI visibility — even for universal fields

8. **Universal objects are network-level, but each campaign needs the field *enabled* to SEE it.** `campaigns_tag_categories` (GraphQL `isInCampaign`) gates profile rendering and the search picker per campaign. Universal **writes land regardless** (proven: writes to a field enabled in zero campaigns landed and listed via API — the inverse of the campaign-local silent drop: data lands but is invisible). Universal-field *values* auto-enable wherever their field is enabled (their per-campaign checkboxes render disabled-checked); campaign-local fields still need value-level activation (`campaigns_tags`, the §2.1 story).
   - **A new universal field starts enabled in ZERO campaigns**, and new campaigns start with zero enablement rows even for pre-existing universal sections (this is also what §2.1 saw in `campaigns_tag_groups`).
   - **The field-level checkbox in Customize → Info is inert for universal fields** — it toggles visually but fires no mutation and doesn't persist. Working primitive: GraphQL `addTagCategoryToCampaign(input: {campaignId, tagCategoryId})` (inverse: `removeTagCategoryFromCampaign`) — verified: enabled field 31 in Test and the profile rendered it immediately.
   - **Enablement audit (BQ, 2026-08-13): `Trainings > Organizing For Power` is enabled in ZERO campaigns** — the universal OFP field is invisible network-wide despite 828 tagged entities and nightly sync writes (organizers, including OT, cannot see it — half the point of the universal cutover). `EP > Shifted` is enabled in the 22 original states but NOT VA/DC. 1MC fields: only Organizing Team + Test. **Fix is a ~26-mutation replay of `addTagCategoryToCampaign` per field** — Rob 2026-08-13: approved, fold into Phase 2 (non-urgent).
   - **Migration consequence:** Phase 2 gains a step — after creating Activity/Interests fields, enable each in all campaigns via the mutation; add "enable universal fields" to the new-campaign checklist; extend the `phantom_tag_writes` mindset with a field-enablement audit query (visibility gaps don't drop data but hide it).

---

## 4. Design principles

1. **Two kinds of sections, nothing else.**
   - **Program sections** (universal): durable, additive facts about a person's history with a named program — EP, 1MC, Trainings. New program → new universal section, same pattern.
   - **Engine sections**: the machinery — *Activity* (universal, sync-replaced metrics), *Engagement* (campaign-local, rotating sync flags), *Interests* (human-entered preferences), *Local Groups* (campaign-local, state-specific structures).
2. **Universal by default.** A field is campaign-local only when it *must* be: (a) it needs true tag removal (rotating flags), or (b) it is inherently campaign-specific (local group membership). Everything else goes universal — kills per-campaign setup, kills the VA/DC failure class, and gives OT full visibility.
3. **One owner per field: the sync or humans, never both.** Sync-owned fields will be overwritten nightly; human edits there are futile. Human-owned fields are never touched by the sync. Document the split (one-pager for organizers) rather than encoding it in ugly names.
4. **Value names are self-describing, globally unique, and searchable in isolation.** Assume the reader sees only the value name (§5).
5. **Grow inside the structure.** New platform metric → new value in Activity. New training → new field in Trainings. New state group → new field in Local Groups. New program with durable person-facts → new universal program section. Nobody should ever need to invent a section ad hoc.

---

## 5. Naming conventions (the search-driven part)

Rules:

- **A value name must locate itself with zero context.** No bare years ("2024"), no bare dates ("1/1/2025"), no generic words ("Opt-In", "RSVP Date", "Meeting").
- **Unique across the whole network** — a name is never reused in another field (avoids the current OFP/Storytelling/Total Conversations collisions, and means any search hit is unambiguous).
- **Time windows are part of the name** for metrics: "(Past 6 Months)", "(All Time)". A number without its window is uninterpretable.
- **Program prefixes** make families of values co-discoverable — typing the prefix lists the family; typing the specific term still finds the value. Err long; length is cheap, ambiguity isn't.

Prefix registry (extend as programs are added):

| Prefix / pattern | Family | Example |
|---|---|---|
| `EP Shift Worked <year>` | Election Protection shift history | EP Shift Worked 2024 |
| `OFP Training: <competency>` | Organizing For Power trainings | OFP Training: Storytelling |
| `1MC <thing>` | Million Conversations | 1MC Host, 1MC Total Conversations |
| `Interest: <activity>` | Volunteer interests | Interest: Phone Banking |
| `Issue: <bucket>` | Issue interests | Issue: Voting & Fair Representation |
| `<Metric> (<window>)` | Activity metrics | Action Network Actions (Past 6 Months) |
| `Top <scope> <what>` | Rotating spotlight flags | Top State Action Taker |
| `<ST> Group: <name>` | State/local group membership | NE Group: Omaha |

Both directions of the original complaint are covered: typing "2024" still finds "EP Shift Worked 2024", and typing "EP" now finds the whole EP family.

---

## 6. The proposed taxonomy

Seven person sections total (5 universal + 2 campaign-local) plus the existing connection section. Today's 12 person sections → 7, of which only **one** (Engagement) must be created per new campaign, and one (Local Groups) only in states that want it.

### 6.1 Universal — engine

**Section: `Activity`** — *new universal section; sync-owned; replace-on-write; writes explicit 0 instead of clearing.*
(Named "Activity" rather than "Participation" to avoid colliding with the legacy section during transition — see decision D2.)

| Field | Values (type) | Source |
|---|---|---|
| Event Attendance | First Event Attended (date); Most Recent Event Attended (date); Events Attended (Past 6 Months) (number) | `mobilize_event_data` |
| Online Actions | Action Network Actions (Past 6 Months) (number); State Action Network Actions (Past 6 Months) (number); NewMode Actions (All Time) (number); Soapboxx Stories (All Time) (number) | AN / NewMode / Soapboxx models |
| Phone Banking | Phone Bank Calls Made (All Time) (number) | `scaletowin_call_data` |

Notes: window suffixes verified against the models (NewMode, Soapboxx, and ScaleToWin counts are all-time; Mobilize count is 6-month). Being universal, these become visible in campaign 26 and every future campaign — after per-campaign field enablement (§3 item 8) — the OT-visibility fix. Edge case accepted: one network-wide value per entity (a person in two state campaigns shows one State-AN count, not per-state values; today's per-campaign duplicates make this moot in practice).

**Post-E2 layout adjustment (2026-08-13):** single-select means **one response per entity per FIELD** (E2: writing a second value replaces the first), and universal fields can't use delete-and-rewrite — so mutable metrics can't share a field. The three grouped fields above would either accumulate stale values (multiselect) or collide (single-select). Create instead **one single-select field per mutable metric**, field named for the metric with a single like-named value — the existing "Total Conversations" pattern: First Event Attended (date, write-once) · Most Recent Event Attended (date) · Events Attended (Past 6 Months) (number) · Action Network Actions (Past 6 Months) · State Action Network Actions (Past 6 Months) · NewMode Actions (All Time) · Soapboxx Stories (All Time) · Phone Bank Calls Made (All Time) — 8 single-select fields in Activity. Search UX unchanged (value names are what users search). One untested corner: **date-type replace-on-write** (number + standard verified; date presumed same) — verify on the real Most Recent Event Attended field with a Testy write immediately after creation, before any sync wiring.

**Section: `Engagement`** — *existing campaign-local section, reused and extended; sync-owned; add/remove flags.*
Stays campaign-local **on purpose**: these flags rotate (people fall off lists), and removal is impossible for universal taggings. Also correctly campaign-scoped semantically — "hot prospect" is a *this-campaign's-call-list* concept.

| Field | Values | Source / behavior |
|---|---|---|
| Prospect Identification (existing) | Hot Prospect (existing) | `hot_prospects`; added and removed nightly. (1MC prospect statuses stay in the 1MC section per D8 — earlier drafts moved them here.) |
| Top Performers (new) | Top State Action Taker; Top National Action Network Activist (moved from Participation, names kept) | top-performer models; added and removed nightly |

**Section: `Interests`** — *new; human-owned; consolidates Volunteer Activity Interest + Expressed Volunteer Interest + Issues.*
Universal **if experiment E1 shows organizers can remove universal taggings in the UI** (people change their minds); otherwise campaign-local. Rob's 2026-06-11 design rule — human-entered shared statuses belong in universal sections — argues universal.

| Field | Values |
|---|---|
| Volunteer Interests | Interest: Phone Banking · Interest: Poll Monitoring · Interest: Poll Worker · Interest: Canvassing · Interest: Event Volunteering · Interest: Legal Monitoring · Interest: Electoral Count Monitoring · Interest: Clerk & BOE Outreach (LEAP) · Interest: Election Day EP Volunteering · Interest: Petition Signature Gathering · Interest: EP Swag Captain (2026) |
| Issue Interests | Issue: Voting & Fair Representation · Issue: Accountability & Anti-Corruption · Issue: Civil Rights & Civil Liberties (extend to the full CC issue-bucket list) |

Dropped, pending sign-off (D5): "Level of Volunteer Activity Interest" (0 uses), "Recent Activism" (3 uses — the Activity metrics say this better).

**Section: `Local Groups`** — *new; campaign-local, created only in states that use it; human-owned.*
Replaces the per-state one-off sections (Action Team, Chapters and Subgroups). Rule: a new state structure is a new **field** here, never a new section.

| Field | Values | Migrates from |
|---|---|---|
| Nebraska Regional Groups | NE Group: Lincoln · NE Group: Omaha · NE Group: District 8 (NE team should confirm the 3 apparent District-8 duplicates collapse to one) | Chapters and Subgroups (2 taggings) |
| PA Action Team | PA Action Team Member | Action Team > Opt-In (86 taggings) |

### 6.2 Universal — programs

**Section: `Election Protection`** — *existing universal section; becomes sync-owned.*

| Field | Values | Notes |
|---|---|---|
| Election Protection Shifts (rename of "Shifted") | **EP Shift Worked 2024** (rename of "2024" — the 1,428 existing taggings come along, pending E4) · **EP Shift Worked 2022** (new) · **EP Shift Worked 2026** (new) | Additive, one value per cycle worked |

Today this field is a partial manual import (1,428 tagged vs. thousands of shift-workers loaded by the EP qualifiers). Proposal: the sync owns it — stamp `EP Shift Worked <year>` from the same three qualifier branches that already feed `master_load_qualifiers` (`ep_qualifiers`, `ep_2022_qualifiers`, `ep_2026_qualifiers`), including a one-time backfill. Additive-only, so universal-safe, and 2026 values land as shifts happen.

**Section: `1 Million Conversations`** — *existing universal section; slimmed.*

| Field | Values | Notes |
|---|---|---|
| Million Conversations Role | 1MC Host · 1MC Leader · 1MC Participant (renames; 0 taggings today, so free) | Additive; sync-written from Mobilize trainings / Airtable |
| Total Conversations | 1MC Total Conversations (number; rename) | Mutable but universal-safe via replace-on-write + explicit-0 |
| Conversation Notes | 1MC Host Conversation Notes · 1MC Event Attendee Notes · 1MC Event Host Notes (renames) | Notes-type; append-only via `append_notes` |
| 1MC Prospect Status (**new single-select field — CREATED 2026-08-13**, category id 31) | 1MC Host Prospect · 1MC Leader Prospect · 1MC Prospect: None (**live**; tag ids 98/99/100) | **Stays here per D8 (Rob 2026-08-13)** — not moved to Engagement. Universal ⇒ API-undeletable, so the sync rotates status via **replace-on-write single-select — E2-verified live**: each write replaces the prior status; "1MC Prospect: None" is the explicit off-list sentinel (standard-tag analog of D1's explicit-0). Existing multiselect "Million Conversations Prospect" field gets archived (multiselect can't be flipped post-creation; its stray "Total Conversations" value is already archived). Field still needs `addTagCategoryToCampaign` enablement in Organizing Team (+ any campaign that should see it) before sync wiring — currently enabled in Test only. |

Timing win: the `million_conversations_*` columns are not yet in `TAG_COLS` — wire them up pointing at the new names and the old ones never go live.

**Section: `Trainings`** — *existing universal section; values renamed for search.*

| Field | Values |
|---|---|
| Organizing For Power | OFP Training: Organizing Basics · OFP Training: Storytelling · OFP Training: Relational Organizing · OFP Training: Rapid Response Basics (renames of the 4 competencies; taggings & hardcoded interact_ids survive per E4) |
| *(future trainings = new fields here)* | e.g. Poll Monitor Training → "Poll Monitor Trained (2026)" |

Fixes the "Storytelling" search collision (the bare value name currently ties OFP to Soapboxx).

### 6.3 Connection tags

**Section: `Organizer Relationships`** (OT) — unchanged: Assigned Organizer > Regional Organizer (493 connections). Fits the taxonomy as-is.

### 6.4 Retired after migration

Sections: Participation (old), Activism, Issues, Expressed Volunteer Interest, Event RSVP Dates, Action Team, Chapters and Subgroups. Plus the dead Participation fields (Event Participation, Event Participation Type, First Event Attendance, Specific Events Attended). (The 1MC Prospect field is no longer retired — it stays in the 1MC section per D8.) Whether "retired" means archive-only or archive-plus-mass-delete depends on experiment E3 (do archived values pollute search?). Mass-deleting ~350K campaign-local taggings at 4 calls/sec is multi-day but parallelizable by campaign; archive-only is obviously preferable if search hides archived values.

---

## 7. Migration plan (phased, each phase independently shippable)

**Phase 0 — VA/DC remediation + guardrail (urgent, independent of the redesign):**
- Decide: create the *old* Participation/Engagement sections in VA/DC now, or fast-track this proposal and let the universal Activity section cover them at cutover (recommended if cutover is weeks, not months — only Engagement then needs hand-creation in VA/DC/Test).
- Purge or annotate the ~24K phantom `add_tagging ok` rows for campaigns 24/25 in `sync_log` so the overlay stops reporting values that don't exist. (If we cut over to new tag names, the masking self-heals for the new fields — new names have no phantom history — but the log should still be corrected.)
- Ship a **phantom-write detector**: dbt model flagging `sync_log` ok `add_tagging` rows with no matching `taggable_logbook` row after N days, grouped by campaign × tag. Catches this entire failure class network-wide, forever.

**Phase 1 — Experiments (E1–E4, §3). ✅ DONE 2026-08-13 — all four pass** (results inline in §3). Interests goes universal; D8 rotation confirmed (field already created); archive-only cleanup; renames safe. Bonus: field-enablement model discovered (§3 item 8).

**Phase 2 — Create, rename & enable in the UI.**
- Create universal sections: Activity, Interests (E1 passed → universal).
- Create Engagement fields (Top Performers) + add Engagement to VA/DC/Test; create Local Groups in NE/PA.
- Renames (coordinate each with its code constant, same day): EP field+value, OFP values, 1MC values (Host → 1MC Host already done as the E4 test). **The sync writes by name** — a UI rename without the matching code change breaks matching (or silently creates nothing).
- **NEW (per §3 item 8): enable every universal field in every campaign** via `addTagCategoryToCampaign` replay (the Customize checkbox is inert for universal fields). Includes the standing gaps: Trainings > OFP (currently enabled NOWHERE), EP > Shifted in VA/DC, 1MC fields beyond OT/Test as desired, and the new `1MC Prospect Status` (Test-only today). Add "enable universal fields" to the new-campaign checklist; re-run the enablement audit query after any campaign or field is added.

**Phase 3 — Repoint the pipeline (one PR).**
- dbt: sync-string section/field/name constants in the source models and `correct_participation_values`; tag-name lists in `current_tag_values(_bq_only)`; `hot_prospects` / top-performer section moves. 1MC-prospect writes stay pointed at the 1MC section (universal replace-on-write + "None" sentinel semantics, per D8/E2). `TAG_COLS` routing is untouched (internal column names ≠ AB taxonomy).
- `sync.py`: update `INSERT_TAG_FIELDS`; add per-field **universal write semantics** (skip the DELETE step; on "Clear Value" write explicit 0 instead) — the ofp_tag additive path already models half of this; update OFP/1MC name constants; enable the 1MC `TAG_COLS` entries with the new names.
- New: `ep_shift_tags` model (additive `EP Shift Worked <year>` from the three qualifier branches) + insert-time stamping; add the EP tag to the removal-safe additive path.
- One-time: EP shift-year backfill run.

**Phase 4 — Cutover & verify.** Let the nightly populate the new fields; verify per-campaign entity counts new-vs-old (inventory queries in the appendix); confirm VA/DC receive their first real taggings.

**Phase 5 — Migrate the small manual datasets, then retire.** One-off script rewrites the ~120 live human-entered taggings (PA Action Team 86, Volunteer interests 19, Issues 9, NE groups 2 — check `created_by` and ping owners first; RSVP Dates 9 and Recent Activism 3 proposed dropped, D5). Then archive (± mass-delete per E3) everything in §6.4.

**Phase 6 — Documentation.** `docs/sync_overview.md` field table, the new-campaign checklist (now: create Engagement section, done), the organizer-facing "who writes what" one-pager, MEMORY.md.

---

## 8. Decision points — DECIDED by Rob 2026-08-13

All recommendations accepted as written, with one modification (D8).

| # | Decision | Outcome (2026-08-13) |
|---|---|---|
| D1 | Universal Activity section: "explicit 0, never clear" + one network-wide value per entity? | **Accepted** — explicit 0, never clear |
| D2 | Name for the new metrics section | **`Activity`** |
| D3 | Interests: universal (pending E1) or campaign-local? | **Universal** — E1 passed 2026-08-13 (UI delete of universal taggings works and persists) |
| D4 | Rename list (EP "2024", OFP values, 1MC values) | **Approved** — batch each rename with its code change |
| D5 | Drop RSVP Dates (9), Recent Activism (3), Level of Volunteer Activity Interest (0)? | **Drop.** Courtesy check run 2026-08-13: 13 live taggings — Recent Activism ×3 by Cheech Sorilla (user 10), RSVP Dates ×10 by Flose LaPierre (13) / Franceska Edouard (15); newest Nov 2025, all ≥9 months stale. Rob to ping owners or call it stale, then archive in Phase 5 |
| D6 | VA/DC: patch old sections now, or leapfrog? | **Already resolved 2026-08-07** — patched via activation (§2.1 status note); detector + log fix shipped |
| D7 | Ownership marking: docs/training vs name suffixes | **Docs/training** — names stay clean |
| D8 *(new)* | 1MC prospect statuses: Engagement (as drafted) or the 1MC section? | **Rob's modification: keep them in the 1MC section** (§6.2). Rotation via replace-on-write single-select + explicit "1MC Prospect: None" sentinel — **E2 PASSED 2026-08-13**; the `1MC Prospect Status` field is live with all three values |

---

## Appendix: inventory queries

```sql
-- Sections → fields → values with usage
SELECT g.name AS section, c.name AS field, t.name AS value_name, t.tag_type,
       COUNT(DISTINCT IF(l.deleted_at IS NULL, l.taggable_id, NULL)) AS entities
FROM actionbuilder_cleaned.cln_actionbuilder__tags t
JOIN actionbuilder_cleaned.cln_actionbuilder__tag_categories c ON t.tag_category_id = c.id
JOIN actionbuilder_cleaned.cln_actionbuilder__tag_groups g ON c.tag_group_id = g.id
LEFT JOIN actionbuilder_cleaned.cln_actionbuilder__taggable_logbook l ON l.tag_id = t.id
GROUP BY 1,2,3,4 ORDER BY 1,2,3;

-- Which campaigns have which sections (universal sections have no rows here)
SELECT g.name AS section, COUNT(DISTINCT ctg.campaign_id) AS n_campaigns,
       STRING_AGG(DISTINCT ca.name ORDER BY ca.name LIMIT 30) AS campaigns
FROM actionbuilder_cleaned.cln_actionbuilder__campaigns_tag_groups ctg
JOIN actionbuilder_cleaned.cln_actionbuilder__tag_groups g ON g.id = ctg.tag_group_id
LEFT JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns ca ON ca.id = ctg.campaign_id
GROUP BY 1 ORDER BY 1;

-- The VA/DC phantom-write evidence
SELECT ca.name, s.operation, s.status, COUNT(*) n, MIN(DATE(s.executed_at)) first_op, MAX(DATE(s.executed_at)) last_op
FROM actionbuilder_sync.sync_log s
JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns ca ON ca.interact_id = s.campaign_interact_id
WHERE ca.id IN (24, 25) GROUP BY 1,2,3 ORDER BY 1,2,3;
```
