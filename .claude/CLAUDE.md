This project manages the `actionbuilder_sync` BigQuery dataset via dbt. Views feed participation data from Mobilize, Action Network, ScaleToWin, NewMode, Soapboxx (storytelling), and Election Protection into ActionBuilder (Common Cause's organizing CRM). Our own sync script — `scripts/sync.py`, built on the `ccef_connections` `ActionBuilderConnector` (it replaced the original TMC consultant script) — reads `updates_needed` and makes ActionBuilder OSDI API calls.

## Documentation map (read before diving into code)

These docs describe how the sync actually operates — prefer them over re-deriving from the SQL/Python:

- **`docs/sync_overview.md`** — **start here.** Canonical end-to-end architecture: the data pipeline (platform staging views → `correct_participation_values` + `current_tag_values` → `updates_needed` → `sync.py`), the sync-string format, tag-removal mechanics, the full inventory of synced fields, and the step-by-step recipe for **incorporating a new participation platform** (tag + load qualifier + hot prospects).
- **`docs/actionbuilder_tags.md`**, **`docs/actionbuilder_person_signup.md`** — verbatim ActionBuilder OSDI API reference (tags + Person Signup Helper).
- **`docs/assessment_rules.md`**, **`docs/deduplication.md`** — assessment logic; the March 2026 dedup analysis.
- **`docs/ab_ui_automation.md`** — agent-driven AB web-UI automation (for work the API can't do: sections/fields, saved queries, email deletes), the GraphQL replay pattern, and the field-activation model.
- **`docs/taxonomy_migration_runbook.md`** — the Blocks A–I tag-taxonomy migration: ID matrices, per-block execution record, hazards. **The old→new field/value mapping lives here** — consult it before trusting any field name in older docs or comments. `docs/tag_taxonomy_redesign_proposal.md` is the approved proposal behind it.
- **`docs/march_on_washington_list.md`** — the campaign-26 recruitment list: frozen-list-outside-dbt pattern and the post-march teardown.

## Tag taxonomy — the Blocks A–H redesign (executed August 2026)

The single biggest structural change to this project in 2026. Assume older docs and code
comments predate it:

- **New sections `Activity`, `Engagement`, `Trainings`** replace the legacy **`Participation`**
  section, which is **retired and gone** (one of 3 sections retired). 18 of 23 legacy fields
  retired; enablements went 261 → 37. Five human-blocked migration fields remain.
- **Most synced fields are now `universal`** — one network-level tag object shared by every
  campaign, rather than a campaign-local object. Three consequences that bite repeatedly:
  1. A universal tagging is **API-undeletable**; single-select replaces on write. Never emit
     a removal for one — it 404s.
  2. A universal tagging reads back through `current_tag_values` with **`campaign_id` NULL**.
     Any join keyed on `campaign_id` silently misses it and re-emits forever (this produced
     219 undrainable OFP rows, and the same bug in three 1MC models on 2026-08-25).
     Anti-join at **entity grain** for universal fields.
  3. Universal fields still need per-campaign **field enablement** to be visible, and an
     unactivated value write returns **200 and silently drops** — see `phantom_tag_writes`.
- **Key on `tag_interact_id`, never on `tag_name` or numeric `tag_id`.** Names reach BQ stale
  (replication lags renames by days) and numeric ids break silently on recategorisation.
  Canonical ids live in `scripts/sync.py` (`TAG_INTERACT_IDS`, `UNIVERSAL_TAG_IDS_NO_DELETE`).
- **BQ cannot audit enablement**, and value archival does not reliably replicate — read both
  live via GraphQL, not from `cln_actionbuilder__tags.status`.

## BigQuery MCP

The global `bigquery` MCP is active and pre-approved for this project. Use `bq_query(sql)` and `bq_list_tables(dataset)` to inspect views, spot-check data, or debug sync issues without leaving the conversation. Connects to `proj-tmc-mem-com` using the shared service account.

Example:
```
bq_query("SELECT * FROM actionbuilder_sync.updates_needed LIMIT 5")
bq_list_tables("actionbuilder_sync")
```

## Schema MCP (bq-schema-docs)

The global `schema` MCP provides field-level documentation for all 63 datasets in `proj-tmc-mem-com`. Use it to look up table structure before writing queries — faster than reading schema files directly.

```
schema_list_datasets()                                                           # master index of all datasets
schema_get_dataset("actionbuilder_sync")                                         # README + data model overview
schema_list_tables("actionnetwork_cleaned")                                      # all table names in a dataset
schema_get_table("actionnetwork_cleaned", "cln_actionnetwork__users")            # all fields + types
schema_search("email_address", dataset="actionnetwork_cleaned")                  # find tables by keyword
```

All tools are pre-approved — no confirmation needed. Docs are auto-generated from INFORMATION_SCHEMA.

## Running dbt

All dbt commands go through `dbt.sh → run_dbt.py`, which loads credentials from `.env`:

```bash
bash dbt.sh run          # deploy all views
bash dbt.sh run -s <model>
bash dbt.sh test
bash dbt.sh compile
```

Do NOT run `dbt` directly — it won't have credentials.

## ActionBuilder Instance

- Subdomain: `commoncause`
- Web UI base: `https://commoncause.actionbuilder.org`
- Entity profile URL pattern: `https://commoncause.actionbuilder.org/entity/view/{entity_id}/profile?campaignId={campaign_id}&clientQueryId=null`
  (uses internal numeric entity_id and campaign_id, not interact_ids)
- API base: `https://commoncause.actionbuilder.org/api/rest/v1`

## AB Web-UI Automation (ab-ui MCP)

For work the API can't do (create sections/fields/queries/tasks, delete emails), agents drive
the web UI directly — see **`docs/ab_ui_automation.md`**. The `ab-ui` Playwright MCP server
(project-local, Rob's machine) exposes browser tools against an authenticated session; the
session layer is `scripts/ab_ui_session.py` (`check` headless liveness / `login` scripted +
headless via `ACTION_BUILDER_WEB_PW` in `.env` — agents recover dead sessions themselves).
Auth state lives at `~/.ab-ui/storage_state.json` — never in the repo. Prefer
`browser_snapshot` over screenshots; UI writes are live production edits (get sign-off for
bulk/destructive changes).

## Credentials

- `.env` in project root holds `BIGQUERY_CREDENTIALS_PASSWORD` (full service account JSON, one line, no quotes)
- Never `source .env` in bash — the JSON will break the shell
- `run_dbt.py` handles credential loading safely

## Library Policy — ccef-connections first

All BigQuery and external-service access in Python scripts MUST go through `ccef_connections` connectors (`BigQueryConnector`, `ActionBuilderConnector`, etc.). Do NOT use `google.cloud.bigquery`, `google.oauth2`, or other service SDKs directly. This keeps credential handling, retry logic, and connection patterns consistent across all CCEF projects.

Pattern for scripts that need BQ:
```python
from dotenv import load_dotenv
from ccef_connections.connectors.bigquery import BigQueryConnector

load_dotenv(dotenv_path='.env')   # call before constructing any connector
bq = BigQueryConnector(project_id='proj-tmc-mem-com')
bq.connect()
rows = list(bq.query("SELECT ..."))
```

The only exception is `bigquery.ScalarQueryParameter` for parameterized queries — avoid even this by inlining validated, non-user-supplied values directly into the SQL string.

## Current State (as of 2026-08-25)

- 24 state campaigns active (22 original + VA and DC added 2026-04-09) plus Test campaign, plus the crosscutting **Organizing Team** campaign (id 26, `1e7e58fd-efb4-4810-91dc-2e7aac08625a`) — NOT a state campaign; keep out of state routing
- **VA/DC silent tag-write drop: REMEDIATED 2026-08-07.** AB fields are network-level objects activated per campaign per tag VALUE (`campaigns_tags`); VA/DC had zero activations, so ~23.8K `add_tagging ok` writes silently dropped (200 + no-op) April–July. Fixed via UI/GraphQL activation of all sync-written values in 24/25; phantom sync_log rows annotated `status='phantom'` (overlay now re-emits); canary write verified landing. Same audit found **Soapboxx Stories activated in ZERO campaigns** (all 9 writes since June dropped) — activated in all 24 campaigns via `AssociateTagToCampaign` replay, plus 36 pre-universal-cutover OFP writes to VA/DC (entities re-stamped by universal field; no data lost). **`phantom_tag_writes` dbt model** now detects this failure class network-wide (expect zero rows; check after adding campaigns/fields). See `docs/ab_ui_automation.md` for the GraphQL replay + activation-model mechanics.
- **Kelly Dufour wrong-email fix: DONE 2026-08-07.** `mlewis@commoncause.org` rewritten to inert unique `removed-mlewis-artifact-e{id}@example.invalid` (unsubscribed) on entities 1343/1388/1541 — AB has no email delete, UI or API; address rewrite severs the identity cross-link. Campaign-less entities reached via temporary connect→edit→remove loop (logged in sync_log, run_id `manual_email_fix_20260807`).
- Nightly workflow: "Nightly ActionBuilder Update" ([Civis #119217](https://platform.civisanalytics.com/spa/#/workflows/119217)) runs daily at 10 PM ET (02:00 UTC), **8–10 hours typical runtime** — measured 2026-08-19 from `sync_log` first-to-last op: 7/07 476 min, 7/10 558 min, 8/19 590 min; the 8/18 post-outage catch-up ran 1,020 min. (The long-standing "1.5–4 hours" figure here was stale and made healthy long runs look like hangs.) A run therefore routinely finishes mid-morning ET and is often still in flight at 9 AM; that is normal, not a symptom. 8 steps — see `civis/SCHEDULED_SCRIPTS.md` (authoritative) and `civis/civis_workflow.yaml` (exported task graph):
  0. AB run_dbt (`bash dbt.sh run`) — [#358132951](https://platform.civisanalytics.com/spa/#/scripts/containers/358132951) — must stay first (table-materialized models)
  1. AB Inserts (`insert_new_records`) — [#345082775](https://platform.civisanalytics.com/spa/#/scripts/containers/345082775)
  2. AB Tag Updates (`update_records`) — [#346138397](https://platform.civisanalytics.com/#/scripts/containers/346138397)
  3. AB Assessment Setting (`apply_assessments`) — [#346528478](https://platform.civisanalytics.com/#/scripts/containers/346528478)
  4. AB Notes Append (`append_notes`) — [#348368977](https://platform.civisanalytics.com/#/scripts/containers/348368977)
  5. AB Organizing Team Connect (`connect_entities`) — [#357827345](https://platform.civisanalytics.com/spa/#/scripts/containers/357827345)
  6. AB Organizing Team Inserts (`insert_organizing_team`) — [#357827433](https://platform.civisanalytics.com/spa/#/scripts/containers/357827433)
  7. AB Organizing Team Assign Organizers (`assign_organizers`) — [#360156836](https://platform.civisanalytics.com/spa/#/scripts/containers/360156836) — all five organizers live, no filter (2026-07-29: Lamair's 141 assigned; Tiffany Rubio covers all unstaffed states + territories, her 71 assigned after `remove_ot_duplicates` removed the 70 campaign-26 double-insert twins)
- OFP field: now the **universal** `Trainings > Organizing For Power` field (one network-level tag object; replaced the archived campaign-local `Activism > Organizing For Power`). Additive-only / no-removal (universal taggings are API-undeletable). OFP attendance is a load qualifier (`ofp_qualifiers` in `master_load_qualifiers`), with state derived from Mobilize zip via `geo_crosswalks_cleaned.cln_geo_crosswalks__zip_county_lookup`. New universal tag interact_ids are hardcoded in `ofp_attendance.sql` and `sync.py` (`OFP_UNIVERSAL_TAG_IDS`).
- **1MC: tag writes LIVE since Block G (2026-08-19)** — the three `million_conversations_*` columns entered `TAG_COLS` then; before that only `append_notes` was live. The whole `1 Million Conversations` section is **universal**. All six `1mc_*` models were reshaped to entity grain + interact_id keying on 2026-08-25 (`2b2876d`); `1mc_prospects` had it from Block G. **Upstream caveat:** the `airtable-bq-sync` job (Civis 347402326) failed 82 consecutive nights 2026-06-04 → 08-25, freezing `million_conversations`; it is fixed. **Program caveat:** 1MC is not currently running as the pipeline assumes — every host is CC staff, 202 of 205 "conversations" are group headcounts with nobody named, and both training-map seeds (`1mc_training_map.csv` here, `mc_event_map.csv` in actionbuilder-analytics) are header-only, so `1MC Host`/`1MC Leader` have never been written by any path. Those zeroes are correct, not bugs. Dashboards live in the **actionbuilder-analytics** project, which reads the AB tag plus (since 2026-08-25) the Airtable mirror directly.
- Sync log: LIVE — instruments all operations with per-tag granularity (`add_tagging`, `delete_tagging`, `insert_entity`, `set_assessment`, `append_note`)
- Sync log overlay: DEPLOYED — `current_tag_values` overlays sync_log on BQ snapshot; still needed for hard-delete gap coverage
- AB mirror bug: RESOLVED ~2026-03-21 — `taggable_logbook` replication restored (was stalled 3/5–3/20). Overlay and BQ-only models diverge by ~10K rows due to timing + hard-delete gap; overlay is more accurate.
- `deduplicated_names_to_load_bq_only`: filters `WHERE first_name IS NOT NULL` to avoid AB API 422 errors on nameless AN-only records
- Removable workaround: VA/DC campaign UUIDs are hardcoded (UNION DISTINCT) in the state_campaign_maps of `deduplicated_names_to_load_bq_only.sql`, `ep_external_removal.sql`, and `mobilize_external_removal.sql` from before BQ replicated campaign ids 24/25. **Verified 2026-07-03: both campaigns now replicate and the name-join resolves them** — the hardcoded rows are redundant (harmless) and safe to delete.
- Mobilize anti-poaching (2026-06-17): `master_load_qualifiers.mobilize_qualifiers` gates against other groups' EP volunteers. **Rule A** — an external-PTV-coded person loads via Mobilize only with an independent non-Mobilize CC touch (subscribed AN / NewMode / Soapboxx / ScaleToWin / CC-coded PTV); unsubbed AN never counts (also tightens `cc_engaged_emails` via `subscribed_an_emails`). **Rule B** — a signup whose `referrer__utm_source` matches an external PTV code doesn't qualify. **OFP exempt** (separate `ofp_qualifiers`). One-shot cleanup of already-loaded: `mobilize_external_removal` → `remove_mobilize_externals` op (manual, NOT in nightly) — **executed 2026-06-18: 98 removed, ok=98 err=0**; feed self-cleared via the removal-gap overlay.
- External PTV source codes (2026-06-18): the canonical set is `external_ptv_source_codes` (used by `external_ep_emails`, `cc_coded_ep_emails`, the Mobilize `external_source_codes` in all three external-aware models). Robust to `ep_archive.source_codes` data issues: (1) case collisions — a code is external only if NO casing is flagged internal (internal wins); (2) explicit known-ours override list. **`CCAZ`/`CCAZR` = Common Cause Arizona were mis-flagged `external='Y'`** there; the upstream rows can't be edited from this project (no write access to `ep_archive`), so they're overridden in the model. Drop them from the override once the source table is corrected.

## BQ Replication Gaps (known, reported to TMC 2026-03-12)

1. **Hard deletes never replicated** — `campaigns_entities` removals have no `updated_at` change;
   BQ perpetually shows removed entities. This is permanent; sync_log overlays compensate.
   The canonical overlay is **`removed_campaign_entities`** (all entity-from-campaign removal
   ops: `remove_from_campaign`, `remove_ep_external`, `remove_mobilize_external`, status ok/404).
   Every removal feed subtracts it (`dedup_candidates`, `ep_external_removal`,
   `mobilize_external_removal`) so already-removed entities aren't re-surfaced. **When you add a
   new removal op to sync.py, add its operation string to `removed_campaign_entities.sql`.**
   (This is also why a removal feed showing N rows ≠ N live entities: re-running is 404-tolerant.)
2. **`taggable_logbook` replication — RESOLVED** ~2026-03-21. AB fixed their internal SQL mirror.
   Data flowing again; 107K+ rows replicated since the fix. The overlay model remains deployed
   because it also handles the hard-delete gap (#1) and provides fresher values between replication cycles.
3. **Value archival does not reliably replicate** (found 2026-08-20). `Activism > Organizing For Power`
   tags 76–79 report `archived: true` in AB GraphQL but are still `status = 1` in
   `cln_actionbuilder__tags`, `updated_at` untouched since creation (2026-03-27) — two months after
   being archived at the June universal cutover. Older archives (tags 1–5, 37–39, 85) did land as
   `status = 0`. **Consequence: never archive a value expecting it to drop out of a model that filters
   `WHERE t.status = 1`** (i.e. `current_tag_values_bq_only`). Archive for UI hygiene; change pipeline
   behaviour in code. Renames DO replicate (Blocks D–F), just with a multi-day lag.

## Sync Log Architecture

Three dbt models exist in paired versions:
- `dedup_candidates_bq_only` — original BQ-only logic (used by dedup_ambiguous)
- `dedup_candidates` — thin wrapper filtering out entities already logged as removed
- `deduplicated_names_to_load_bq_only` — original BQ-only logic
- `deduplicated_names_to_load` — thin wrapper filtering out person_ids already logged as inserted
- `current_tag_values_bq_only` — original BQ-only tag state from taggable_logbook
- `current_tag_values` — overlay merging sync_log tag ops onto stale BQ snapshot

To revert to BQ-only mode: delete the wrapper models, rename `_bq_only` files back,
revert dedup_ambiguous ref. The sync_log table stays as a permanent audit log.

### sync_log columns (tag-level, added 2026-03-20)
- `tag_name` — human-readable tag name (e.g. "Events Attended Past 6 Months")
- `value_written` — the value written (number as string, date, or 'applied'); on `insert_entity` rows (since 2026-07-29) it holds the inserted email, feeding the email-keyed already-inserted guards in `deduplicated_names_to_load` and `organizing_team_inserts`
- Operations: `add_tagging`, `delete_tagging` (in addition to existing entity-level ops)

## Key Datasets

| Dataset | Role |
|---------|------|
| `actionbuilder_sync` | This project's output (all views) |
| `actionbuilder_cleaned` | Cleaned AB database tables |
| `core_enhanced` | Cross-platform identity hub |
| `mobilize_cleaned` | Mobilize event participation |
| `actionnetwork_cleaned` | Action Network users/actions |
| `scaletowin_dialer_cleaned` | ScaleToWin calls |
