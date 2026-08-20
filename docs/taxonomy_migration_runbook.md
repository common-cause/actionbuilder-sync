# Taxonomy Migration Runbook (Phases 2–6)

**Companion to:** `docs/tag_taxonomy_redesign_proposal.md` (APPROVED 2026-08-13; experiments E1–E4 all passed — see §3 there).
**Purpose:** each block below is one sitting's work — execute the block, run its checks, stop clean. Blocks A–C are purely additive (safe any day). Blocks D–F are rename days (each must land with its code change before that night's 10 PM ET nightly). Block G is the one code cutover.
**Status:** READY 2026-08-13 — all blocks fully specified (code-reference inventory swept the repo at HEAD `2327a3c`; line numbers below are from that tree — re-verify with grep if the files have moved).
**How the sync actually keys on names (drives every rename rule below):** tag ADDs go by NAME (sync string → signup helper; AB resolves the new name instantly), tag DELETEs go by interact_id, and every read/idempotency path — `updates_needed`'s tag-name filters, per-model `WHERE tag_name` filters, `current_tag_values`' name joins, `OFP_UNIVERSAL_TAG_IDS` dict keys — is NAME-keyed and goes blind at the moment of a UI rename, producing duplicate writes and skipped removals rather than errors. Hence: every rename of a sync-read name ships with its code edit the same day, before 10 PM ET.

---

## 0. Standing facts

### Campaign matrix (numeric id · interact_id · name)

| id | interact_id | campaign |
|---|---|---|
| 1 | `0e41ca37-e05d-499c-943b-9d08dc8725b0` | Test |
| 2 | `e37684a0-1284-49b5-b4aa-855d9faa5ae2` | Nebraska |
| 3 | `a00b53e0-1ffb-4692-a347-58fe1ad73aa8` | Pennsylvania |
| 4 | `d5c48860-3764-4020-9d21-ac6024daefa0` | Rhode Island |
| 5 | `c998a441-0cc0-405e-a3fe-1b4839ec101a` | Florida |
| 6 | `c7cf1a2b-a9e5-43dd-93a9-928d4bc979e4` | Texas |
| 7 | `8407578c-f147-4d50-a91e-245282bc4aa2` | Michigan |
| 8 | `feb40677-0ed8-4a1e-9fd2-290526dc6ab1` | New Mexico |
| 9 | `37c5ef62-f4de-4769-ae19-624e5ae42ecd` | Ohio |
| 10 | `9f4b8be6-9baf-430d-b548-77227b787f86` | New York |
| 11 | `c04eece0-5e68-410d-8436-7b28690d4fe0` | Colorado |
| 12 | `fd65be58-cce6-400f-97f8-e14adb6558d3` | California |
| 13 | `e8298624-3568-4d92-948b-4429e55d6271` | Oregon |
| 14 | `f6b17bf5-90e2-4252-8e7e-cf11ff3f83a0` | Minnesota |
| 15 | `b6c5d9d8-c382-4da2-85ee-fbf6594d0a04` | Illinois |
| 16 | `51fb121f-a9c6-47a9-a27e-163d0f81b9f2` | Massachusetts |
| 17 | `96dca89a-61bd-49f4-87a8-4368e655f1c3` | North Carolina |
| 18 | `dd6b11e3-d82a-44c5-91bb-ec516c723fd0` | Georgia |
| 19 | `a41cde2c-a06f-4fed-8073-b544ca9aead7` | Arizona |
| 20 | `993e08fe-bdeb-460c-832c-71c1b8c19dba` | Hawaii |
| 21 | `af5fcde6-2b84-48c3-a8bb-7de045ede252` | Indiana |
| 22 | `16702ebe-ddc3-4c80-b832-b9f0a6881f0c` | Maryland |
| 23 | `12951a1f-6d24-4923-ba31-d4aa6c4c3183` | Wisconsin |
| 24 | `261251df-8836-4f90-a9fb-fdd5dc1798b1` | Virginia |
| 25 | `3a227511-fd6f-40f6-abfc-4f2c05ff3b91` | District of Columbia |
| 26 | `1e7e58fd-efb4-4810-91dc-2e7aac08625a` | Organizing Team |

### Taxonomy object ids (internal numeric — what the GraphQL mutations take)

| Object | cat_id | tag ids (status=1 unless noted) |
|---|---|---|
| EP > **Election Protection Shifts** (field renamed from `Shifted`, Block F) | 9 | EP Shift Worked 2024 = **45** (renamed from `2024`, 1,428 taggings preserved) · EP Shift Worked 2022 = **129** · EP Shift Worked 2026 = **130** — universal, so all three auto-enable wherever cat 9 is enabled (26/26) |
| 1MC > Million Conversations Role | 24 | 1MC Host = 81, Leader = 80, Participant = 82 |
| 1MC > Million Conversations Prospect (to archive) | 25 | Host Prospect = 84, Leader Prospect = 83 (+ archived stray 85) |
| 1MC > Total Conversations | 26 | Total Conversations = 86 |
| 1MC > Conversation Notes | 27 | 87 / 88 / 89 |
| 1MC > **1MC Prospect Status** (new, single-select) | **31** | 1MC Host Prospect = 98, 1MC Leader Prospect = 99, 1MC Prospect: None = 100 |
| Trainings > Organizing For Power | 29 | Organizing Basics = 91, Rapid Response Basics = 92, Relational Organizing = 93, Storytelling = 94 |
| Engagement > Prospect Identification | 22 | Hot Prospect = 75 |
| Participation (legacy, sync-written) | 6/7/16/18/21/28 | 40, 41, 43, 42, 60, 65, 73, 74, 64, 90 |
| **Activity** (universal section, group id **13**; created 2026-08-13) > First Event Attended (Date) | 32 | 101 |
| Activity > Most Recent Event Attended (Date) | 33 | 102 |
| Activity > Events Attended (Past 6 Months) (Number) | 34 | 103 |
| Activity > Action Network Actions (Past 6 Months) (Number) | 35 | 104 |
| Activity > State Action Network Actions (Past 6 Months) (Number) | 36 | 105 |
| Activity > NewMode Actions (All Time) (Number) | 37 | 106 |
| Activity > Soapboxx Stories (All Time) (Number) | 38 | 107 |
| Activity > Phone Bank Calls Made (All Time) (Number) | 39 | 108 |
| **Interests** (universal section, group id **14**; created 2026-08-14) > Volunteer Interests (Standard, multi) | 40 | Phone Banking 109 · Poll Monitoring 110 · Poll Worker 111 · Canvassing 112 · Event Volunteering 113 · Legal Monitoring 114 · Electoral Count Monitoring 115 · Clerk & BOE Outreach (LEAP) 116 · Election Day EP Volunteering 117 · Petition Signature Gathering 118 · EP Swag Captain (2026) 119 *(all prefixed `Interest: `)* |
| Interests > Issue Interests (Standard, multi) | 41 | `Issue: ` Voting & Fair Representation 120 · Accountability & Anti-Corruption 121 · Civil Rights & Civil Liberties 122 |
| **Engagement > Top Performers** (campaign-local, Standard multi, readonly like its sync-owned sibling cat 22) | 42 | Top State Action Taker = 123, Top National Action Network Activist = 124 *(activated campaigns 1–25)* |
| **Local Groups** (campaign-local section, group id **15**) > Nebraska Regional Groups | 43 | NE Group: Lincoln 125 · Omaha 126 · District 8 127 *(activated campaign 2 only)* |
| Local Groups > PA Action Team | 44 | PA Action Team Member = 128 *(activated campaign 3 only)* |

### Mechanics cheat-sheet (all E-verified — proposal §3)

- **Creation-time-only settings:** universality (section level), field type, **single vs multi select**. Cannot change after creation — get them right at creation.
- **Universal fields:** writes land everywhere regardless of enablement; **visibility requires the field enabled per campaign** (`addTagCategoryToCampaign`). The Customize field-level checkbox is INERT for universal fields — always use the mutation. Universal values auto-enable wherever their field is enabled.
- **Campaign-local fields:** activation is per VALUE (`AssociateTagToCampaign(campaignId, tagId)`); unactivated writes 200-and-silently-drop (`phantom_tag_writes` catches).
- **Single-select = one response per entity per field**, replace-on-write. Multiselect accumulates across values (same-value rewrite dedupes).
- **Renames preserve interact_id + taggings.** Sync writes by NAME → every rename of a sync-written value pairs with its code change, same day, before 10 PM ET.
- **Archive hides from search**; archive-only cleanup, no mass deletes.
- **GraphQL replay:** in-page `fetch('/api/graphql')` with devise-token-auth headers from localStorage (`accessToken`, `client`, `uid`, `token-type: Bearer`) — see `docs/ab_ui_automation.md`. Introspection is enabled if a mutation is unknown.
- **Taxonomy objects can be created by GraphQL, not just by UI clicking** (proven in Block C — 2 fields + 14 values + 52 enablements in three calls):
  - `createTagCategory(input: {name, tagGroupId, multiselectable!, locked!, allowToCreateTagType!, multiselectSameTagBehavior, readonly, attachmentsEnabled, isUniversal})` — `allowToCreateTagType` ∈ `Standard|Number|Date|Notes|Address|Signature|Shift`; `multiselectSameTagBehavior` ∈ `append|overwrite` (**`overwrite` = the UI's "Allow multiple of the same response? → Don't Allow"**).
  - `createTag(input: {name, tagCategoryId, tagType, campaignId?})` — `tagType` matches the field's type.
  - `createTagGroup(input: {name, targetType, targetTypeId, isUniversal})` — the two `target*` args are unresolved; **create SECTIONS in the UI** (`/admin/fields` → + Add Section) and everything below them via GraphQL.
  - Read-back queries: `getTagGroups { nodes { id name isUniversal categories { id name locked multiselectable readonly allowToCreateTagType tags { id name tagType archived } } } }`, and `getTagCategoryById(tagCategoryId: "<id>")` — note the arg is **`tagCategoryId`**, not `id`. `associatedCampaignIds` on a category/tag shows current activation without waiting for BQ.
  - **Always mirror a live reference field of the same kind** (cat 24 universal standard-multi; cat 22 sync-owned campaign-local) and verify the mutation's echoed settings — creation-time-only options (universality, type, single-vs-multi) cannot be fixed later.

### Pre-flight, every session

```bash
.venv/Scripts/python.exe scripts/ab_ui_session.py check   # ab-ui alive (login to recover)
git status                                                 # clean tree before UI+code days
```

### Enablement replay snippet (browser_evaluate, adjust ids)

```js
async () => {
  const h = {'content-type':'application/json',
    'access-token': localStorage.getItem('accessToken'),
    'client': localStorage.getItem('client'),
    'uid': localStorage.getItem('uid'), 'token-type':'Bearer'};
  const CAMPAIGNS = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26];
  const CATS = [29];                       // <-- field cat_ids for this run
  const out = [];
  for (const c of CAMPAIGNS) for (const cat of CATS) {
    const r = await fetch('/api/graphql', {method:'POST', headers:h, body: JSON.stringify({
      operationName:'AddTagCategoryToCampaign',
      variables:{input:{campaignId:String(c), tagCategoryId:String(cat)}},
      query:`mutation AddTagCategoryToCampaign($input: AddTagCategoryToCampaignInput!) {
        addTagCategoryToCampaign(input: $input) { tagCategory { id isInCampaign(campaignId: "${c}") } } }`})});
    const j = await r.json();
    out.push(`${c}/${cat}: ` + (j.data?.addTagCategoryToCampaign?.tagCategory?.isInCampaign ?? JSON.stringify(j.errors||j).slice(0,80)));
  }
  return out.join('\n');
}
```

Verify from the mutation's own `isInCampaign` response, never the 200. (Value-level activation replays are identical with `AssociateTagToCampaign(input:{campaignId, tagId})`.)

---

## V. Verification toolkit (run the relevant ones after every block)

**V1 — Universal-field enablement audit** (expect: every listed field enabled in all 26; BQ lags a day — same-day, spot-check `isInCampaign` via GraphQL instead):

```sql
WITH universal_cats AS (
  SELECT c.id, CONCAT(g.name,' > ',c.name) AS field
  FROM actionbuilder_cleaned.cln_actionbuilder__tag_categories c
  JOIN actionbuilder_cleaned.cln_actionbuilder__tag_groups g ON g.id = c.tag_group_id
  WHERE g.name IN ('1 Million Conversations','Trainings','Election Protection','Activity','Interests')
)
SELECT uc.field, COUNT(DISTINCT ctc.campaign_id) AS n_enabled,
       STRING_AGG(DISTINCT CAST(26 - NULL AS STRING)) AS _  -- placeholder; list missing via NOT IN if n < 26
FROM universal_cats uc
LEFT JOIN actionbuilder_cleaned.cln_actionbuilder__campaigns_tag_categories ctc
  ON ctc.tag_category_id = uc.id
GROUP BY 1 ORDER BY 1;
```

**V2 — Phantom writes** (expect zero rows, always): `SELECT * FROM actionbuilder_sync.phantom_tag_writes`

**V3 — Count-reconciliation baseline (live entities per value, captured 2026-08-13).** After cutover, distinct-entity counts on the NEW values must converge to ≥ these (minus genuine 6-month-window decay):

| Old value | Entities |
|---|---|
| Action Network Actions | 46,952 |
| First Event Attended / Most Recent Event Attended | 18,836 each |
| Events Attended Past 6 Months | 9,595 |
| Action Network State Actions | 6,675 |
| Phone Bank Calls Made | 2,185 |
| NewMode Actions | 1,776 |
| Top State Action Taker | 934 |
| Top National Action Network Activist | 94 |
| Soapboxx Stories | 1 (post-remediation re-emit still draining) |
| **Distinct entities, any sync-written value** | **53,599** |

**V4 — Profile spot check:** one OFP-tagged entity in a state campaign, one Activity-tagged entity in Test + one state, campaign-26 view for 1MC Prospect Status.

---

## Block A — Enablement backfill — ✅ EXECUTED 2026-08-13 (134 mutations, all `isInCampaign: true`; V4 OFP render verified on entity 56592/Indiana; V2 zero post-nightly; GraphQL re-read spot-checks true 2026-08-14 — BQ V1 still pending replication)

1. `addTagCategoryToCampaign` replay (snippet above):
   - cat **29** (Trainings > OFP) → all 26 campaigns *(currently enabled NOWHERE — the invisibility fix)*
   - cat **9** (EP > Shifted) → campaigns **24, 25, 26** (22 states already have it; Test optional — include for testability)
   - cats **24, 26, 27, 31** (1MC Role / Total Conversations / Notes / Prospect Status) → all 26 *(universal-by-default doctrine; cat 31 needed in 26 before Block G wires the sync)*
   - **Skip cat 25** (old Million Conversations Prospect — being archived; don't spread it)
2. Checks: mutation responses all `isInCampaign: true` → V4 OFP profile spot-check → V1 in BQ next day.

Rollback: `removeTagCategoryFromCampaign`, same shape.

---

## Block B — Create the Activity section (8 single-select fields) + date-type check — ✅ EXECUTED 2026-08-13/14 (section universal, group id 13; ids in §0; **E5 PASS** — date replace-on-write verified on Testy, test tagging UI-removed + API-verified gone; all 8 cats enabled × 26 campaigns, 208 mutations true; V4 Activity render verified on Testy/Test)

1. `/admin/fields` → **+ Add Section** → name `Activity`, **universal** (this is our first section creation — if the universal option does NOT appear at section creation, STOP and investigate; everything downstream assumes it).
2. Create 8 fields, each: **type per table, Single select, "Allow any user to add new responses" OFF** (locked), read-only OFF. Then one response each, named exactly:

| # | Field name | Type | Response (value) name | Source (Block G) |
|---|---|---|---|---|
| 1 | First Event Attended | Date | First Event Attended | mobilize_event_data |
| 2 | Most Recent Event Attended | Date | Most Recent Event Attended | mobilize_event_data |
| 3 | Events Attended (Past 6 Months) | Number | Events Attended (Past 6 Months) | mobilize_event_data |
| 4 | Action Network Actions (Past 6 Months) | Number | Action Network Actions (Past 6 Months) | AN models |
| 5 | State Action Network Actions (Past 6 Months) | Number | State Action Network Actions (Past 6 Months) | AN state models |
| 6 | NewMode Actions (All Time) | Number | NewMode Actions (All Time) | NewMode model |
| 7 | Soapboxx Stories (All Time) | Number | Soapboxx Stories (All Time) | Soapboxx model |
| 8 | Phone Bank Calls Made (All Time) | Number | Phone Bank Calls Made (All Time) | scaletowin_call_data |

3. **Capture the new cat_ids + tag ids** (GraphQL `SearchTags`/`GetTagGroups2` responses, or the network tab) → record them in §0 above.
4. **E5 — date replace-on-write check** (the one unverified corner): write `Most Recent Event Attended = 2026-01-01` to Testy McTesterson (entity `a0624225-…`, Test campaign) via `update_entity_with_tags`, then overwrite with `2026-02-01`; `list_person_taggings` must show ONE tagging with the new date. Then UI-delete it (Remove Info). If dates do NOT replace → STOP; fields 1–2 need a redesign (e.g. number-typed YYYYMMDD) — back to Rob.
5. Enable all 8 new cat_ids → all 26 campaigns (replay snippet).
6. Checks: V4 (field renders on a Test profile), mutation `isInCampaign` responses, V1 next day.

---

## Block C — Interests, Top Performers, Local Groups — ✅ EXECUTED 2026-08-14 (ids in §0; Interests cats 40/41 enabled ×26 = 52 true; 51 value activations true; V2 zero; scoping verified both ways — NE profile shows Nebraska Regional Groups and NOT PA Action Team, PA profile the inverse)

> **Sections were created in the UI; fields/values/activations via GraphQL** (`createTagCategory` / `createTag` / `associateTagToCampaign` — input shapes in the mechanics notes below). Each new field's settings were mirrored from a live reference field of the same kind (cat 24 for universal standard-multi, cat 22 for sync-owned campaign-local) and read back from the mutation response.
>
> **Two judgment calls made during execution — flag if either is wrong:**
> 1. **`Top Performers` created `readonly: true`**, matching its sync-owned sibling Prospect Identification (cat 22). Organizers see the flags, admins/API write them. Precedent says API writes are unaffected (cat 22 is readonly and takes nightly Hot Prospect writes).
> 2. **Local Groups fields created MULTI-select** (legacy `Nebraska Regional Group` was single). Group membership is naturally a set, multiselect can express single but not vice-versa, and select mode is creation-time-only. All four values are removable (campaign-local ⇒ true delete).
>
> Also found: legacy `Nebraska Regional Group` (cat 17) is `locked: false` — that unlocked field is how organizers created the three District-8 variants. All new fields are `locked: true`.
>
> **Hot Prospect (tag 75) was already activated in VA/DC** by the 2026-08-07 Phase 0 remediation, so step 3 reduced to Test (campaign 1) only — done.

1. **Interests** (universal section — D3): create section `Interests`, universal. Fields, both **Standard · Multi select** (people hold several; humans remove via UI per E1), locked OFF? — keep **locked ON** (only admins add new response options; organizers still apply/remove them on people):
   - `Volunteer Interests`: Interest: Phone Banking · Interest: Poll Monitoring · Interest: Poll Worker · Interest: Canvassing · Interest: Event Volunteering · Interest: Legal Monitoring · Interest: Electoral Count Monitoring · Interest: Clerk & BOE Outreach (LEAP) · Interest: Election Day EP Volunteering · Interest: Petition Signature Gathering · Interest: EP Swag Captain (2026)
   - `Issue Interests`: Issue: Voting & Fair Representation · Issue: Accountability & Anti-Corruption · Issue: Civil Rights & Civil Liberties
   - Enable both cat_ids → all 26 campaigns.
2. **Engagement > Top Performers** (campaign-local; rotation via true delete works here): new field `Top Performers`, **Standard · Multi select**, locked. Values: `Top State Action Taker`, `Top National Action Network Activist` (names kept — no code rename needed). Then **value-level activation**: `AssociateTagToCampaign` for both new tag ids × campaigns 2–23 (+ 24, 25 once they have Engagement, next step) + 1 (Test).
3. **Engagement into VA/DC/Test**: `AssociateTagToCampaign(campaignId ∈ {24,25,1}, tagId=75)` (Hot Prospect) + the two Top Performers values — this is what creates Engagement's junction rows there (value-level activation cascades field/section on).
4. **Local Groups** (campaign-local section, human-owned): create section `Local Groups` (NOT universal). Fields: `Nebraska Regional Groups` (values: NE Group: Lincoln · NE Group: Omaha · NE Group: District 8) and `PA Action Team` (value: PA Action Team Member). Activate NE values in campaign 2, PA value in campaign 3 only.
5. Capture all new ids into §0. Checks: V2; response-picker search shows the new value names in an enabled campaign; V1 next day for Interests.

---

## Block D — Rename day: OFP values (+ code, same day, BEFORE 10 PM ET) — ✅ EXECUTED 2026-08-17 (commit `84bf334`)

> Renames done by **GraphQL `updateTag(input: {tagId, name})`** — no UI clicking needed (input also takes optional `tagType`/`archived`; omitting them leaves them untouched, verified on tag 92 first). Read-back confirmed all four renamed with `tagType`, `archived: false`, cat-29 universality/multi/`overwrite`/locked and all 26 campaign enablements unchanged; live API on a Georgia entity returned the new names on existing taggings (E4 re-confirmed in production).
>
> **Deviation from the spec below — `ofp_attendance` was NOT a comment-only edit.** Its idempotency join compared `tag_name`, not the `tag_interact_id` it filters on, so post-rename every existing tagging looked "missing". Measured: the name-only join matched **0 of 1,452** rows after the rename (the accepted "re-add once" quirk was really a full-feed re-emit). `current_ofp_tags` now derives the value name from the interact_id via CASE, so the join is rename-proof; feed stayed at its pre-rename **720** rows. Future renames of these values need only the seed + CASE literals kept in step.
>
> Also verified: the new names contain `: ` but never the `:|:` delimiter, so `parse_sync_string` (splits on `:|:`, then `parts[3].split(':', 1)`) and `_extract_tag_info` round-trip them correctly; a live canary write of `OFP Training: Rapid Response Basics` to Testy (Test campaign) landed on interact_id `1ef15001-…`. V2 phantom_tag_writes = 0. Testy keeps that canary tagging (universal ⇒ API-undeletable; UI-removable if wanted).
>
> ✅ **RESOLVED 2026-08-18 — the nightly ran and Block D's deferred checks all passed.** The 2026-08-18 run wrote 43,647 `add_tagging` / 39,738 `delete_tagging` / 1,294 `insert_entity` (the big delete count is normal replace-on-write churn for number fields after a five-week gap). V2 `phantom_tag_writes` = **0**, and OFP writes landed under the NEW names (`OFP Training: Organizing Basics` 198, `Storytelling` 149, `Relational Organizing` 146) — Block D confirmed correct in production.
>
> ✅ **The OFP churn loop is FIXED — confirmed drained 2026-08-19.** Nightly OFP adds ran an identical 192/147/144 every night up to the outage, wrote 198/149/146 on 8/18, and wrote **zero** on 8/19; `ofp_attendance` now sits at its 13-row Test-campaign floor. Block D's interact_id-keyed join plus PR #2's `(entity, tag)` graining closed it.
>
> ⚠️ **Method note, worth more than the fix itself:** on 8/18 this was recorded here as "survived the fix, still running." That was wrong. 8/18 was simultaneously the first nightly after a five-week outage and the first under renamed values, so a full re-emit was expected whether or not the loop was fixed — the observation could not tell the two apart. **When the first run after a change is also a catch-up run, judge steady state from run N+1.**
>
> Also from the 8/18 run: **17 `set_assessment` errors** (gone on 8/19 — 1,855 ok, 0 errors) and **463 `delete_tagging` rows with a NULL `tag_name`**.
>
> ⚠️ **New on 8/19, still open: 8,494 `delete_tagging` 404s** (vs 7,772 ok), 8,038 of them `Action Network Actions`. Historically 404s are rare (97 on 7/07, none most nights). Most likely the BQ `taggable_logbook` mirror had not replicated the 39,738 deletions from the 8/18 catch-up when 8/19's dbt ran, so `updates_needed` offered removal_ids for taggings already gone. Harmless (404-tolerant, and the tagging is already in the desired state) but ~8.5K wasted calls. **If 8/20 still shows thousands, the sync_log overlay has a real suppression gap** — that is the night that decides it.
>
> ⚠️ **Original finding (2026-08-17), now historical: the nightly sync had written nothing since 2026-07-13.** `sync_log` shows `add_tagging`/`insert_entity`/`set_assessment` stopping after 2026-07-13; everything logged since (7/29, 7/30, 8/07) is manual ops. Upstream is healthy — Mobilize participations current to 8/17, AB entity mirror updating to 8/16 — so this is the Civis workflow (#119217) not running or failing before it writes, not a data problem. Block D's "next morning" checks below assume a nightly that runs; they cannot pass until this is resolved. Separately, before it stopped the OFP feed was in a **churn loop** — the identical 483 taggings (192/147/144, never `Rapid Response Basics`) rewritten every night without draining — worth diagnosing when the nightly is restored.

UI renames in Trainings > Organizing For Power (E4-safe; interact_ids survive):
`Organizing Basics` → `OFP Training: Organizing Basics` · `Storytelling` → `OFP Training: Storytelling` · `Relational Organizing` → `OFP Training: Relational Organizing` · `Rapid Response Basics` → `OFP Training: Rapid Response Basics`
(The Storytelling rename also kills the OFP-value/Soapboxx-field search collision.)

Paired code changes — one commit, merged + `bash dbt.sh seed && bash dbt.sh run` before 10 PM:

| File | Lines | Change |
|---|---|---|
| `seeds/ofp_training_map.csv` | `ofp_tag` column, all data rows | old value names → new (this seed feeds the sync strings in `ofp_attendance.sql:84` and `ofp_universe.sql:83` — **the rename IS a seed edit**; requires `dbt seed`) |
| `scripts/sync.py` | 155–160 | `OFP_UNIVERSAL_TAG_IDS` dict KEYS → new names (values/UUIDs unchanged) — miss this and `_get_tag_map`'s override goes stale |
| `scripts/sync.py` | 1237–1243 | `managed_tag_names` → new OFP names (and fix the pre-existing staleness: add `Soapboxx Stories` while in there) |
| `models/updates_needed.sql` | 63–66 | the four OFP names in the `tag_name IN (…)` filter |
| `models/current_tag_values_bq_only.sql` | 97–100 | the four OFP name branches |
| `models/current_tag_values.sql` | 212–223 | **add** the four OFP branches with the NEW names — they're missing here today (drift vs `_bq_only`; fix the drift in this commit) |
| `models/ofp_attendance.sql` | 68–71 | comment labels only (interact_ids rename-immune — this is the idempotency path that keeps OFP safe during the transition) |
| `scripts/add_ofp_field_to_campaigns.py` | whole file | **DELETE** — legacy footgun; re-running it would recreate the archived `Activism > Organizing for Power` field in every campaign |

Do NOT touch: `field_group = 'Organizing for Power'` (lowercase `for`) at `updates_needed.sql:529` / `ofp_attendance.sql:80` — internal routing token, not an AB name.

Sequence: code commit ready → UI renames (4) → merge → `bash dbt.sh seed` → `bash dbt.sh run` → nightly. Next morning: V2 zero (pre-rename sync_log rows match by interact_id, so no false positives expected — investigate any that appear), OFP canary write lands under a new name, sync_log ok counts normal. Transitional quirk (accepted): `updates_needed`'s stale current-value view may re-add an OFP tag once; same-tag rewrite dedupes (`multiselectSameTagBehavior: overwrite`), no data effect.

---

## Block E — Rename day: 1MC values (mostly free — one exception: notes) — ✅ EXECUTED 2026-08-18 (commit `3581930`)

`Leader` → `1MC Leader` · `Participant` → `1MC Participant` (Role, tags 80/82; `1MC Host` done 2026-08-13) · `Total Conversations` value 86 → `1MC Total Conversations` · Conversation Notes 87/88/89 → `1MC Host Conversation Notes` / `1MC Event Attendee Notes` / `1MC Event Host Notes`.

> Six `updateTag(input: {tagId, name})` calls (canary on tag 80 first, then the remaining five). Read-back confirmed all three categories unchanged: universal, locked, correct `allowToCreateTagType`, cat 24/27 multi + cat 26 single, `associatedCampaignIds` still 26/26, nothing archived. Note `updateTag`'s payload has **no `tagCategory` field** — including it fails document validation, which aborts before execution (safe, but costs a round trip).
>
> **The notes re-key was real, and measurable.** Executing the compiled `1mc_notes` right after the rename returned **3 rows** — every historical note queued to re-append, exactly as predicted. The three `sync_log` rows were re-keyed with one idempotent `UPDATE` (suffix-matched on `':<old response name>'`, so re-running can't double-prefix) and the model returned to **0**. Post-deploy: 0.
>
> **Pre-existing drift this block surfaced and fixed:** `updates_needed.sql:68` and `auto_assessment_rules.sql:111,114` still filtered on `'Host'`, which Block A renamed to `1MC Host` on 2026-08-13 — those reads had been blind for five days. Impact nil (tags 80/81 have zero live taggings, so `has_host_tag` was already always FALSE), but it is the same failure mode this runbook exists to prevent: a Block-A/B/C rename with no code pairing because the value looked unused.
>
> Live tagging counts at rename time (why this block was low-risk): 80 Leader **0**, 82 Participant **4**, 86 Total Conversations **4**, 87 **9**, 88 **2**, 89 **2**. The three `million_conversations_*_tag` columns are confirmed absent from `sync.py` `TAG_COLS`, so Role/TC renames cannot race a write path — only `append_notes` is live.
>
> `seeds/1mc_training_map.csv` is still header-only, so no `dbt seed` was needed; `1mc_role_attendance.sql` now carries a comment that any future row must use the new names, since `role_tag` is carried verbatim into the sync string.
>
> Validation: all six changed models executed via their compiled SQL (per the `dbt compile` lesson), then `dbt.sh run -s <models>+` → PASS=8. Deployed views verified to contain the new names and no old literals. V2 phantom_tag_writes = 0.
>
> ⚠️ One authoring gotcha: a `⚠️` emoji in a model comment crashed dbt's console logger on Windows (cp1252) while it echoed the compiled node. Keep model comments ASCII.

Inventory findings that shape this block:

- **Tag columns aren't written yet** (`million_conversations_*_tag` emitted by `updates_needed.sql:534/539/544` but absent from `sync.py` `TAG_COLS:96–104`) — so Role/TC renames can't race the write path. But the **dbt read filters run nightly** and should be updated in the same commit for hygiene: `updates_needed.sql:67–71` (`'Leader','Host','Participant','Total Conversations','Host Prospect'` in the tag_name list), `1mc_participants.sql:64,71,73,75`, `1mc_role_attendance.sql:60,69`, `1mc_total_conversations.sql:78,84–86`, `auto_assessment_rules.sql:111–114` (Host/Leader), and the future-data convention for `seeds/1mc_training_map.csv` `role_tag` (header-only today — seed rows must use the NEW names when populated).
- **`1mc_prospects.sql` is NOT renamed** — it gets rewritten wholesale in Block G for `1MC Prospect Status` (D8). Leave `Host Prospect` (tag 84) un-renamed in AB; its field (cat 25) archives in Block H.
- **⚠️ Conversation Notes renames DO race a live path.** `append_notes` reads section/field/response_name from `models/1mc_notes.sql` (lines 57, 92, 121, 179–180) and — the subtle part — its **idempotency key is `{airtable_record_id}:{response_name}` stored in `sync_log.tag_name`** (`1mc_notes.sql:44,187`). Renaming the three response values re-keys every historical note → re-append. Volume is tiny (~11 notes today), so either: (a) accept ~11 duplicate note appends, or (b) same-day `UPDATE actionbuilder_sync.sync_log SET tag_name = REPLACE(...)` to re-key the historical rows. **Do (b)** — it's one statement per response name. Pair the UI renames with the `1mc_notes.sql` literals + the sync_log re-key, same day, before 10 PM.

Verify next morning: `append_notes` step logs 0 appends (no re-key misses), V2 zero.

**Next morning (2026-08-19) check — still open at time of writing:** confirm the `append_notes` step logged **0** appends. Any non-zero count means a re-key miss and those notes were appended twice; the duplicates would need removing by hand.

---

## Block F — Rename day: Election Protection (+ new values) — ✅ EXECUTED 2026-08-18 (no code change; nothing to commit but docs)

Field `Shifted` → `Election Protection Shifts`; value `2024` (tag 45, 1,428 taggings — they survive per E4) → `EP Shift Worked 2024`; **+ Add Response** `EP Shift Worked 2022`, `EP Shift Worked 2026`.

**Inventory verdict: zero code references.** No sync string, filter, or constant anywhere touches `Shifted` or the `2024` tag (the `shifted_2024` columns in `master_load_qualifiers` etc. are `ep_archive` source columns, unrelated to the AB tag). The sync first learns these names in Block G's `ep_shift_tags` model — which must be written against the NEW names. Renames + new values can happen any day with no code pairing. Capture the two new tag ids for §0.

> **Executed.** Re-verified the zero-reference claim before touching anything: every `shifted` hit in the repo is the `ep_archive` source column `shifted_2024`, and the three `Election Protection` hits are Mobilize `organization__name` values — no AB section/field/value literal anywhere. So no commit beyond docs, and no `dbt run`.
>
> **New mutation: `updateTagCategory(input: {tagCategoryId, name})`** renames a FIELD (`updateTag` is values only). Introspected `UpdateTagCategoryInput` first, because a category carries creation-time-only settings that cannot be repaired if a mutation resets them — only `tagCategoryId` is required, and **`isUniversal` / `allowToCreateTagType` are not in the input at all**, so they cannot be disturbed. Read-back confirmed: universal, multi, locked, Standard, `overwrite`, 26/26 campaigns, all unchanged.
>
> New ids: **129** = `EP Shift Worked 2022`, **130** = `EP Shift Worked 2026` (`createTag` with `tagCategoryId: "9"`, `tagType: "Standard"`, no `campaignId` — universal values need none).
>
> **Universal auto-enablement verified by canary write, not by assumption.** Tag-level `associatedCampaignIds` reads **0 for all three values — including tag 45, which has 1,428 live working taggings**. That 0 is normal for universal values (activation lives at the field level); it is not a VA/DC-style misconfiguration. Proof: a canary write of `EP Shift Worked 2026` to Testy (Test campaign) LANDED — read back as section `Election Protection` / field `Election Protection Shifts` / name `EP Shift Worked 2026`, tagging `d00f75da-…`. So a value created minutes earlier with zero per-campaign activation accepts writes, which is what Block G's `ep_shift_tags` depends on. Testy keeps that tagging (universal ⇒ API-undeletable; UI-removable).
>
> ⚠️ **Two API gotchas worth remembering** (both cost a round trip here): `update_entity_with_tags` takes the **parsed tag dict** (`action_builder:section` / `action_builder:field` / `name`), NOT a sync string — passing a raw sync string returns **500**. And the section for cat 9 is `Election Protection` (tag group 4); the renamed `Election Protection Shifts` is the FIELD. A wrong section name also returns 500 rather than silently dropping — noisy failure, which is the good case.

---

## Block G — Pipeline cutover PR (Phase 3; one PR, merged before that night's nightly) — ✅ EXECUTED 2026-08-19 (commit `5bd0193`); cutover nightly ran clean 2026-08-19/20

Prereqs: Blocks A–F done; Activity fields exist + enabled everywhere (B); `1MC Prospect Status` enabled in campaign 26 (A).

> ✅ **Cutover nightly verified 2026-08-20.** Ran 02:11–12:59 UTC (**10 h 48 m** — inside the corrected 13–14 h expectation), all 8 steps. **54,259 `add_tagging` ok / 0 errors**, every write under a new name, and — checked by joining `sync_log.tag_interact_id` back to `cln_actionbuilder__tags`, not by name — every write landed on the new tag id (101–108, 123/124, 129/130, 45, 75, 82, 86, 93/94). **`BLOCK_G_TAG_IDS` is proven at scale, not just on the Test canary.** 44,326 distinct entities = **83% of the 53,599 V3 baseline** in one night. V2 `phantom_tag_writes` = 0. Also: the EP shift backfill happened by itself in the first run (`EP Shift Worked 2026` 1,487 / `2022` 1,058 / `2024` 3), so that one-time follow-up is done, and 8/19's 8,494 delete-404 wave fell to **35** — it was replication lag, self-cleared, no suppression gap.
>
> ⚠️ **But the cutover is only ~83% complete, and two Block-G-adjacent defects were found in the 2026-08-20 audit — read Block H's prereqs before archiving anything.** Summary: the WRITE path was made collision-safe, the READ path was not, so the four duplicated value names (`First Event Attended`, `Most Recent Event Attended`, `Top State Action Taker`, `Top National Action Network Activist`) still resolve by name. Detail + fix in Block H below.
>
> **PR authored 2026-08-19 on branch `block-g-pipeline-cutover`.** Merging + `bash dbt.sh run` before 10 PM ET is what started the cutover nightly.
>
> Prereqs re-verified against live AB (`list_tags`), not from this doc: all 8 Activity fields + both Top Performers values + all three `1MC Prospect Status` values + all three `EP Shift Worked *` values are present and correctly typed. `1MC Prospect Status` is **universal** and visible in every campaign checked (1/12/16/21/26) — the earlier "Test-only enablement" note is stale.
>
> **Four corrections to the tables below — the tables are wrong, this note is right:**
>
> 1. **`1MC > Total Conversations` must NOT emit a removal.** The §G sync.py table says "remove old value before writing new one". Live AB reports cat 26 as **universal + single-select**: universal ⇒ the tagging is API-undeletable, single-select ⇒ the new number replaces the old one anyway. Block G is what first puts `million_conversations_activity_tag` into `TAG_COLS`, so keeping the removal would have shipped a *new* 404 wave. Now `CAST(NULL AS STRING)`.
> 2. **The four `Activity` removal columns are NULL too**, for the same reason (universal single-select). Removals survive only for the campaign-local `Engagement` fields — `Top Performers` (multi; top-50 rotation genuinely needs a true delete) and `Prospect Identification`. Full universality inventory is in the PR body.
> 3. **`test_campaign_update_summary.sql` needs no change.** The table lists lines 29/49, but both filters name only `Most Recent Event Attended` and `First Event Attended` — the two response names Block G does not rename.
> 4. **`_get_tag_map()` needs an override table (`BLOCK_G_TAG_IDS`), which §G did not anticipate.** Two independent problems: (a) **name collisions** — `First Event Attended`, `Most Recent Event Attended`, `Top State Action Taker` and `Top National Action Network Activist` now each exist TWICE with `status=1` (legacy Participation + new home), and `_get_tag_map` builds `{name: interact_id}` from an unordered query, so the id was a coin flip; a wrong interact_id in `sync_log` corrupts the `current_tag_values` overlay that every idempotency check reads. Verified those four are the only duplicates. (b) **unreplicated renames** — BQ still holds pre-Block-E/F names, so `1MC Leader`, `1MC Participant`, `1MC Total Conversations`, the three notes responses and all three `EP Shift Worked *` values are absent *by live name*; tag 45 still reads `2024` and 129/130 are missing entirely.
>
> **One deliberate behaviour change not in the plan:** the two universal *date* fields skip rows whose correct value is empty. A number clears by writing `0`; a date cannot, and a universal tagging cannot be deleted — so such a row would be a permanent no-op that reappears nightly and never drains. Stale dates are left for UI removal.
>
> **Runtime expectation corrected:** `CLAUDE.md`'s "1.5–4 hours typical" is stale. Recent nightlies run **8–10 h** (7/07 476 min, 7/10 558 min, 8/19 590 min), so the cutover night is **~13–14 h**, not ~8. Still clears the next 10 PM ET start.

### dbt changes

| File | Lines (pre-PR) | Change |
|---|---|---|
| `models/correct_participation_values.sql` | 332–347 | all 10 sync strings `Participation > <old field> > <old value>` → `Activity > <new field> > <new value>` (§B table; note the two top-performer strings at 339/347 move to `Engagement > Top Performers > <same value names>` instead) |
| `models/action_network_6mo_actions.sql` | 40 | sync string → `Activity > Action Network Actions (Past 6 Months) > …` |
| `models/state_action_network_top_performers.sql` | 157, 162 | 157 → Activity (state AN count); 162 → `Engagement:\|:Top Performers:\|:Top State Action Taker:…` |
| `models/action_network_national_top_performers.sql` | 22 | → `Engagement:\|:Top Performers:\|:Top National Action Network Activist:…` |
| `models/current_tag_values_bq_only.sql` | 86–95 | 10 name branches → new value names + new sync-string map (leave line 96 Hot Prospect; 97–100 OFP already renamed in Block D) |
| `models/current_tag_values.sql` | 212–222 | same 10 branches (post-Block-D file state) |
| `models/updates_needed.sql` | 51–72, 81–104, 255–402 | tag-name filter list + both pivots + `field_name` labels → new value names. `field_group` routing tokens (256…396, 499–547) are INTERNAL — leave them, including `'Prospect Engagement'` and lowercase `'Organizing for Power'`. Add routing for the EP column (new token + CASE branch + `_remove` NULL at the 553–593 mirror) |
| `models/1mc_prospects.sql` | rewrite | target `1 Million Conversations:\|:1MC Prospect Status:\|:<status>:\|:standard_response:<status>`; emit `1MC Prospect: None` when a previously-flagged person drops off; NEVER emit removals; idempotency filter on the three new tag names (or better: tag ids 98/99/100 via interact_id, rename-proof) |
| `models/1mc_participants.sql`, `1mc_role_attendance.sql`, `1mc_total_conversations.sql` | (names updated in Block E) | confirm sync strings carry the new 1MC value names end-to-end |
| `models/ep_shift_tags.sql` | NEW | additive `EP Shift Worked <year>` from the three qualifier branches (`ep_qualifiers`, `ep_2022_qualifiers`, `ep_2026_qualifiers` in `master_load_qualifiers`); idempotency via live-tagging check on the three EP tag ids; feeds a new `ep_shift_tag` column in `updates_needed` |
| `models/test_campaign_update_summary.sql` | 29, 49 | field_name filters → new value names |
| `models/phantom_tag_writes.sql` | 75 | no change needed (interact_id-first match) — but see hazards: pre-rename NULL-iid sync_log rows may name-fallback-miss; annotate `status='renamed'` if any surface |

### sync.py changes

| Lines (pre-PR) | Change |
|---|---|
| 96–104 | `TAG_COLS` += `million_conversations_role_tag`, `million_conversations_activity_tag`, `million_conversations_prospect_tag`, `ep_shift_tag` (`REMOVE_COLS` derives automatically — and `updates_needed` emits NULL removals for all additive/universal columns, so the delete path stays inert for them) |
| 120–139 | `INSERT_TAG_FIELDS` → `Activity` + new field/value names (6 entries; source columns in `deduplicated_names_to_load_bq_only.sql:163–173, 357–368` are internal — unchanged) |
| NEW | **universal write semantics**: for universal single-select fields, "clear" = write explicit 0 (numbers) / `1MC Prospect: None` (prospect status); never call delete_tagging on universal tags (they 404). The additive OFP path already models the no-delete half |
| 1237–1243 | `managed_tag_names` → full new list (Activity values + Hot Prospect + OFP Training: * + 1MC * + EP Shift Worked *) |
| `scripts/targeted_evidence.py:108–115, 250–252` | sync TAG_COLS drift (add `ofp_tag` + the new columns) and docstring examples |
| `scripts/add_tags_to_campaigns.py:16–38` | retire or rewrite — its `NEW_TAGS` literals point at old Participation/Engagement taxonomy |

### Cutover mechanics & expectations
- Merge + `bash dbt.sh run` before 10 PM ET. New names have no logbook history → `updates_needed` re-emits everything: **~53.6K entities ≈ +3.5–4 h on the first nightly** (precedent: VA/DC 23.8K re-emit ≈ +100 min). Acceptable as one long nightly; if staging is preferred, gate the feed by campaign for 2–3 nights.
- Old Participation fields simply stop being written (archived in Block H — no deletes). Old Top State/National taggings in Participation stop being maintained; the Engagement copies take over (old ones archive away in Block H).
- One-time follow-ups after first clean nightly: EP shift-year backfill run; spot-check `1MC Prospect: None` behavior on one real drop-off.
- Next morning: V2 zero; V3 convergence (new-value entity counts vs baseline); sync_log error rate normal; V4 profiles; `updates_needed` drained by nightly 2–3.

---

## Hazards (from the 2026-08-13 code sweep — read before ANY find/replace)

1. **`Storytelling` is overloaded**: Participation FIELD (`correct_participation_values.sql:344`, `sync.py:137`) *and* OFP VALUE (`sync.py:157`, seed). Never global-replace it. (Block D's rename to `OFP Training: Storytelling` retires the collision.)
2. **`Total Conversations` appears in 3 of 4 sync-string positions** (`1mc_total_conversations.sql:86`) — field name stays, value name becomes `1MC Total Conversations`; edit positionally, not by find/replace.
3. **Case trap:** live OFP field is `Organizing For Power` (capital F); the routing token + legacy script use lowercase `for`. Don't normalize.
4. **`field_group` tokens look like AB field names but are internal routing** (`updates_needed.sql:499–593`); renaming them without their CASE partner silently drops rows to no column. Leave them alone.
5. **Drifted duplicates:** `current_tag_values.sql` vs `_bq_only` (OFP branches missing — fixed in Block D); `targeted_evidence.py` TAG_COLS missing `ofp_tag` (fixed in Block G).
6. **`scripts/add_ofp_field_to_campaigns.py`** recreates the archived Activism OFP field if run — deleted in Block D.
7. **Stale shadow tree:** `.claude/worktrees/hot-prospects-weighting/` holds an old copy of models + scripts with old names — EXCLUDE from find/replace; never merge from it. (`target/` compiled SQL regenerates itself.)
8. **Notes idempotency keys embed response names** (`1mc_notes.sql:44,187`) — Block E's re-key step handles it.
9. **Docs carrying old taxonomy** (update in Block I): `docs/sync_overview.md:59–71,218–238,327–330`, `README.md:17,312`, `models/schema.yml` (…61–806), `seeds/schema.yml`, `docs/organizing_team_build_plan.md:98–101`.

Cutover mechanics & expectations:
- Merge + `bash dbt.sh run` before 10 PM ET. New names have no logbook history → `updates_needed` re-emits everything: **~53.6K entities ≈ +3.5–4 h on the first nightly** (precedent: VA/DC 23.8K re-emit ≈ +100 min). Acceptable as one long nightly; if staging is preferred, gate the feed by campaign for 2–3 nights.
- Old Participation fields simply stop being written (they'll be archived in Block H — no deletes).
- Next morning: V2 zero; V3 convergence (new-value entity counts vs baseline); sync_log error rate normal; V4 profiles; `updates_needed` drained by nightly 2–3.

---

## Block H — Migrate manual data, then retire (Phase 5)

> **Audited against live AB 2026-08-20 (post-cutover). Two hard prereqs and three corrections to the steps below — this note is right where it disagrees with them.** Full write-up: artifact "Can We Retire the Old Fields?" (`https://claude.ai/code/artifact/539cdc5f-a394-4dee-9ee7-b830403cf14f`).
>
> **Prereq H0 — fix the read-path name collision FIRST (dbt only, no UI).** `current_tag_values_bq_only.sql:85–105` (and the `sot.tag_name` twin at `current_tag_values.sql:211–223`) map taggings to sync-field identifiers with a `CASE` on **tag_name**. Four names are live in BOTH taxonomies with `status=1` — 40/101 `First Event Attended`, 41/102 `Most Recent Event Attended`, 64/123 `Top State Action Taker`, 74/124 `Top National Action Network Activist` — so a legacy tagging is reported as the NEW field. Measured on entity `0117cc6d` / Michigan: tag **40** (cat 6, Participation) carries `sync_field_identifier = 'Activity:|:First Event Attended:|:…:|:date_response'`, `current_value 2026-07-08`. Consequences, both measured on the cutover night:
>   - **The Activity date fields never populate** — 17 (`First`) and 35 (`Most Recent`) writes vs ~20,009 entities per date. Profile spot-checks show Participation dates present, Activity dates absent.
>   - **Top Performer removals delete the LEGACY tagging** — 399 deletes on tag **64** and 45 on tag **74** (both Participation), while the new Engagement copies accumulate with no removal path. The top-50 rotation depends on that path.
>   Fix = re-key those four branches on the tag interact_id, exactly as Block D did for `ofp_attendance`. Verify next morning: date writes in the thousands, deletes hitting 123/124 not 64/74.
>
> **Prereq H1 — do NOT expect archiving to change pipeline behaviour.** Value archival does not reliably reach BQ: `Activism > Organizing For Power` tags 76–79 report `archived: true` in AB GraphQL but are still `status = 1` in `cln_actionbuilder__tags` with `updated_at` untouched since creation (2026-03-27), two months after being archived. Older archives (1–5, 37–39, 85) did land as `status = 0`. **Third replication gap** — see CLAUDE.md. So archiving is UI hygiene only; the read path must be fixed in code (H0).
>
> **Corrections to the steps:**
> 1. **Migration debt is 135 taggings across 7 fields, not ~120** — and `Action Team Opt-In` (cat 10, tag 46) is **still in active use** (89 taggings, most recent 2026-08-18), so it grows until PA is told to use `Local Groups > PA Action Team`. Counts: Action Team Opt-In **89** · Volunteer Activity Interest (cat 15) **19** · Issue Bucket Interest (cat 11) **9** · Inactives Phonebank RSVP Date (cat 13) **9** · `Million Conversations Prospect` Leader Prospect (cat 25) **4** · Recent Activism (cat 2) **3** · Nebraska Regional Group (cat 17) **2**.
> 2. **TEN fields were free to retire (not twelve — 23 legacy = 7 migrate + 10 free + 6 blocked)** — zero live taggings, no migration, no dependency on H0: cats **4** (29 unused enums — the GA per-event question is a Calendar Events matter, not these), **23** (values already archived; the EMPTY field still rendered in 22 campaigns), **14**, **12**, **20**, **19**, **1**, **3**, **5**, **8**. ✅ **EXECUTED 2026-08-20** — see the wave-1 log below.
> 3. **Retiring = archive the values AND `removeTagCategoryFromCampaign` per campaign.** Archiving alone only hides a value from the picker; the field keeps rendering. Started at **261 field×campaign enablements** across the 23 legacy fields; wave 1 pulled 42, leaving **219**. Per-campaign legacy counts went 9–13 → **7–10** (Test 10 → 6). Whether existing taggings vanish from the profile when their field leaves a campaign is STILL unverified — it does not arise for wave 1 (all zero-tagging) but must be tested before the six duplicated-metric fields come off.

### Block H wave 1 — retire the 10 dead fields — ✅ EXECUTED 2026-08-20

Sequence: canary `removeTagCategoryFromCampaign(campaignId: 1, tagCategoryId: 3)` → read back
`isInCampaign: false`, `associatedCampaignIds: []` → then 41 more removals (**42 total, all verified
false from their own mutation response**) → then **37 orphan value archives** via
`updateTag(input: {tagId, archived: true})`, canary on tag 52 first (**37/37**, zero failures).
Values archived: cat 4 ids 8–36 (29), cat 12 ids 50/96/97, cat 14 id 52, cat 19 id 66, cat 20 ids
70/71/72.

Independent verification (not the mutation responses): `list_tags` on Illinois + Test shows none of
the retired fields in the response picker, and a fresh `getTagGroups` pass shows all ten fields at
`associatedCampaignIds: []` with 0 live values, the new taxonomy untouched (469 enablements, per
campaign still 18/19/17). **`Expressed Volunteer Interest` lost its last field, so that whole section
is gone from staff view.** ⚠️ Watch out when auditing this by field NAME: the retired
`Activism > Organizing For Power` (cat 23) and the live universal `Trainings > Organizing For Power`
(cat 29) share a field name — check the SECTION, or a name-keyed check reports a false positive.

Rollback if ever needed: `addTagCategoryToCampaign` per (campaign, cat) and
`updateTag(input: {tagId, archived: false})` per value.
>
> **Cleared as a risk:** saved queries. All 37 non-temporary queries store tag **ids**, and only two touch legacy tags — `Test Events Filter` (Test, tag 40) and `Rob's Demo Filter` (campaign 10, tags 42/43). The 26 `Hot Prospects` queries use tag 75 and `Intro Phone Bank Search` uses tag 45, both id-stable. No organizer query breaks.
>
> **Also stale (Block I):** `master_load_qualifiers.sql:832–836` still emits OLD field-name label columns (`action_network_field`, `events_field`, `pb_field`, `first_event_field`, `mr_event_field`), carried through `deduplicated_names_to_load_bq_only.sql:167–171,362–366`. Labels only — no read path — but they are wrong names now.

1. One-off script rewrites the ~120 live human taggings to the new homes: PA Action Team Opt-In (86) → `PA Action Team Member`; Volunteer Activity Interest (19) + Expressed Volunteer Interest (7) → `Interest: …` values; Issues (9) → `Issue: …`; NE Chapters (2) → `NE Group: …` (NE team confirms the District-8 triplets collapse). Old taggings are campaign-local → delete via API after re-stamping.
2. D5 drops (Rob decided 2026-08-13; courtesy check done — owners Cheech Sorilla ×3 Recent Activism, Flose LaPierre + Franceska Edouard ×10 RSVP Dates, newest Nov 2025): ping or waive, then no migration — just archive.
3. Archive (archive-only per E3): sections Participation, Activism, Issues, Expressed Volunteer Interest, Event RSVP Dates, Action Team, Chapters and Subgroups; dead Participation fields; old `Million Conversations Prospect` field (cat 25); the 29 Event Participation Type enums.
4. Checks: V2 zero; response-picker search for retired names returns nothing; V3 counts unaffected.

---

## Block I — Documentation (Phase 6)

- `docs/sync_overview.md`: new field inventory + universal-write semantics.
- **New-campaign checklist** (memory `new_campaign_checklist`): add "enable all universal fields in the new campaign (`addTagCategoryToCampaign` × cat list)" + "activate Engagement values" steps.
- Organizer-facing one-pager: who writes what (sync-owned vs human-owned), D7 = docs not name-suffixes.
- Update `CLAUDE.md` Current State, MEMORY.md, KL entry if any.
- Re-run V1 + V2 one week post-cutover.

---

## Rollback notes

- Enablement: `removeTagCategoryFromCampaign` (exact inverse).
- Renames: rename back (interact_ids stable both ways per E4).
- New sections/fields: archive them (hidden from search per E3); universality means they can't be deleted, only archived.
- Code: git revert; `_bq_only` model pair pattern preserves the old logic until we delete it deliberately.
- The sync_log + `phantom_tag_writes` + V3 baseline make any silent regression visible within one nightly.
