# ActionBuilder Sync

Keeps participation data in [ActionBuilder](https://actionbuilder.org) current by syncing activity from Common Cause's other platforms. Built as a dbt project running entirely in BigQuery.

---

## What It Does

Reads participation records from Mobilize, Action Network, ScaleToWin, NewMode, Soapboxx (storytelling), and the EP archive, computes the correct tag values for each activist in ActionBuilder, compares them against what ActionBuilder currently shows, and outputs a table of changes. Our own sync script (`scripts/sync.py`, built on the `ccef-connections` `ActionBuilderConnector`; it replaced the original TMC consultant script) reads that table and makes the ActionBuilder OSDI API calls.

**Current state (2026-07-03):**
- Nightly maintenance — Civis workflow #119217 at 10 PM ET, 8 steps: `run_dbt` → `insert_new_records` → `update_records` → `apply_assessments` → `append_notes` → `connect_entities` → `insert_organizing_team` → `assign_organizers`. See `civis/SCHEDULED_SCRIPTS.md` for the authoritative per-step inventory.
- Tag updates — `update_records` running nightly across all 24 state campaigns (22 original + VA + DC)
- Assessments — `apply_assessments` sets engagement levels automatically (upgrade-only, incl. 1MC Host/Leader criteria); see `docs/assessment_rules.md`
- Organizing Team campaign (id 26) — OFP training attendees connected/inserted nightly; members assigned to their regional organizer via People:People connections (`assign_organizers`)
- New record insertion — nightly, gated by Mobilize anti-poaching rules (other groups' EP volunteers stay out; see `docs/sync_overview.md`)
- 1MC — conversation notes append nightly (`append_notes`); 1MC tag columns staged in `updates_needed` but not yet wired into `sync.py` `TAG_COLS`
- Sync log — live: `actionbuilder_sync.sync_log` records all API operations with per-tag granularity; overlay models compensate for the permanent hard-delete replication gap
- Deduplication — executed March 2026: 154 emails migrated, 91 phones migrated, 8,921 entities removed from campaigns; 16 ambiguous pairs still in `dedup_unresolved`

---

## Setup

### Prerequisites

- Python 3.10+
- dbt-bigquery 1.11+ (`pip install dbt-bigquery`)
- A BigQuery service account with read access to all source datasets and write access to `actionbuilder_sync`
- Git Bash (on Windows) or any Unix shell

### 1. Clone and configure credentials

```bash
git clone <repo-url>
cd "ActionBuilder Sync"
```

Create a `.env` file in the project root:

```
BIGQUERY_CREDENTIALS_PASSWORD={"type":"service_account","project_id":"proj-tmc-mem-com",...}
```

The value is the full contents of your service account JSON key file (on one line, no surrounding quotes).

> **Note:** Do not `source .env` in bash — the JSON value will break. Use `bash dbt.sh` instead, which handles credential loading via Python.

### 2. Verify setup

```bash
bash dbt.sh debug
```

Should report all checks passing.

---

## Running dbt

```bash
# Deploy all models to BigQuery (44 models: 42 views + 2 tables, plus 4 seeds)
bash dbt.sh run

# Deploy a single model
bash dbt.sh run -s dedup_candidates

# Run data tests
bash dbt.sh test

# Compile SQL without deploying
bash dbt.sh compile
```

All commands go through `dbt.sh → run_dbt.py`, which loads `.env`, writes the credential JSON to a temp file, sets `BIGQUERY_KEYFILE_PATH`, and calls `dbt` with whatever arguments were passed.

---

## Project Structure

```
ActionBuilder Sync/
├── models/                     # All dbt models (BigQuery views unless marked TABLE)
│   ├── schema.yml              # Model descriptions and data tests
│   │
│   │   ── Source/staging ──
│   ├── action_network_actions.sql         # Raw AN actions joined to users
│   ├── action_network_6mo_actions.sql     # AN actions filtered to 6 months
│   ├── mobilize_event_data.sql            # Mobilize attendance aggregated by email
│   ├── scaletowin_call_data.sql           # ScaleToWin calls aggregated by phone
│   ├── newmode_actions.sql                # NewMode letter submissions
│   ├── soapboxx_stories.sql               # Soapboxx storytelling submissions
│   ├── ofp_attendance.sql                 # OFP training attendance → universal Trainings field
│   ├── ofp_universe.sql                   # Person-level OFP attendees (base for campaign-26 feeds)
│   ├── external_ptv_source_codes.sql      # Canonical external/internal PTV source-code calls
│   ├── state_action_network_top_performers.sql
│   ├── action_network_national_top_performers.sql
│   │
│   │   ── Core sync pipeline ──
│   ├── correct_participation_values.sql   # "What should be in AB" (computed values)
│   ├── current_tag_values.sql             # "What is in AB" (overlay: BQ + sync_log)
│   ├── current_tag_values_bq_only.sql     # BQ-only version (from taggable_logbook)
│   ├── updates_needed.sql                 # Sync job input — rows to change
│   ├── auto_assessment_rules.sql          # Assessment levels to write
│   ├── hot_prospects.sql                  # High-engagement entities flagged for organizers
│   │
│   │   ── 1 Million Conversations (1MC) ──
│   ├── 1mc_participants.sql               # Airtable 1MC participants resolved to AB entities
│   ├── 1mc_role_attendance.sql            # Host/Leader role training attendance
│   ├── 1mc_total_conversations.sql        # Conversation counts per entity
│   ├── 1mc_prospects.sql                  # 1MC prospect statuses (staged; sync never writes)
│   ├── 1mc_notes.sql                      # Conversation notes feed for append_notes
│   ├── 1mc_entities_to_load.sql           # 1MC people not yet in AB
│   │
│   │   ── Deduplication ──
│   ├── dedup_candidates.sql               # Sync-log filtered wrapper (used by sync.py)
│   ├── dedup_candidates_bq_only.sql       # BQ-only version (used by dedup_ambiguous)
│   ├── dedup_ambiguous.sql                # Ambiguous pairs for human/AI review (TABLE)
│   ├── dedup_unresolved.sql               # dedup_ambiguous minus resolved pairs
│   ├── email_migration_needed.sql         # Emails to copy to keeper entities before deletion
│   ├── phone_migration_needed.sql         # Phones to copy to keeper entities before deletion
│   │
│   │   ── New record insertion ──
│   ├── master_load_qualifiers.sql         # People who qualify for AB entry (TABLE; incl. anti-poaching gates)
│   ├── deduplicated_names_to_load.sql     # Sync-log filtered wrapper (used by sync.py)
│   ├── deduplicated_names_to_load_bq_only.sql  # BQ-only version
│   │
│   │   ── Removals / anti-poaching ──
│   ├── removed_campaign_entities.sql      # Sync-log overlay of already-removed (entity, campaign) pairs
│   ├── ep_external_removal.sql            # One-shot: partner-org EP volunteers to remove (run 2026-06-18)
│   ├── mobilize_external_removal.sql      # One-shot: Mobilize-path external EP volunteers to remove
│   │
│   │   ── Organizing Team campaign (id 26) ──
│   ├── organizing_team_connects.sql       # OFP attendees in AB → connect to campaign 26
│   ├── organizing_team_inserts.sql        # Stateless OFP attendees → insert into campaign 26
│   ├── organizing_team_review.sql         # OFP attendees that can't be routed cleanly
│   ├── organizing_team_assignments.sql    # Members → regional organizer (People:People connections)
│   ├── organizing_team_region_backfill.sql # Campaign-26 entities missing address.state
│   │
│   │   ── Diagnostics ──
│   ├── identity_resolution.sql            # Entity → person_id mapping with data-source flags
│   ├── entity_lookup_debug.sql            # Entity names, emails, phones for spot-checking
│   ├── test_campaign_updates.sql          # Test campaign pending changes (human-readable)
│   └── test_campaign_update_summary.sql   # Test campaign change summary dashboard
│
├── civis/                      # Civis Platform job entrypoints (GitHub-backed)
│   ├── SCHEDULED_SCRIPTS.md         # Authoritative inventory of Civis jobs + workflow steps
│   ├── civis_workflow.yaml          # Exported nightly workflow definition (task graph)
│   ├── run_dbt.sh                   # Nightly step 0: refresh all models before syncing
│   ├── insert_new_records.sh        # Nightly: add new entities
│   ├── update_records.sh            # Nightly: sync tag values
│   ├── apply_assessments.sh         # Nightly: set assessment levels
│   ├── append_notes.sh              # Nightly: append 1MC conversation notes
│   ├── connect_entities.sh          # Nightly: connect OFP attendees to campaign 26
│   ├── insert_organizing_team.sh    # Nightly: insert stateless OFP attendees into campaign 26
│   ├── assign_organizers.sh         # Nightly: assign campaign-26 members to regional organizers
│   ├── snapshot_tag_state.sh        # On-demand: capture tag ground truth from API
│   ├── cleanup_duplicate_tags.sh    # On-demand: remove duplicate taggings
│   ├── remove_ep_externals.sh       # One-shot: remove partner-org EP volunteers (run 2026-06-18)
│   └── remove_duplicate_entities.sh # One-time: dedup execution
│
├── scripts/                    # Python sync scripts
│   ├── sync.py                 # Main sync script (all operations)
│   ├── cleanup_duplicate_tags.py  # Standalone: delete duplicate taggings
│   ├── ai_resolve_dedup.py     # AI-assisted review of dedup_ambiguous pairs
│   ├── add_resolution.py       # Record a manual dedup resolution
│   ├── capture_ab_evidence.py  # One-time: AB bug report (mirror staleness)
│   ├── targeted_evidence.py    # One-time: deletion-check and write-check evidence
│   ├── check_bq_refresh.py     # Utility: check BQ table freshness
│   ├── check_recent_inserts.py # Utility: verify recent entity insertions
│   ├── time_query.py           # Utility: time a BQ query
│   ├── test_connect_semantics.py  # Live-test driver: connect-vs-duplicate API semantics
│   ├── test_notes.py           # Live-test driver: notes append
│   ├── add_tags_to_campaigns.py   # One-time: add tag fields to campaigns
│   ├── add_ofp_field_to_campaigns.py  # One-time: add OFP training field
│   ├── create_sync_log.sql     # One-time DDL to create sync_log table (already run)
│   └── create_dedup_resolutions.sql  # One-time DDL for dedup_resolutions table
│
├── evidence/                   # Output from evidence scripts (March 2026 JSON/TXT reports, committed)
│
├── seeds/                      # dbt seed CSVs
│   ├── schema.yml              # Seed column definitions
│   ├── state_an_thresholds.csv # Per-state AN action thresholds (MI=5, NE=5, default=20)
│   ├── ofp_training_map.csv    # Mobilize timeslot → OFP training name mapping
│   ├── 1mc_training_map.csv    # Mobilize timeslot → 1MC role training mapping
│   └── organizer_state_map.csv # State → C&O organizer (drives assign_organizers)
│
├── docs/
│   ├── sync_overview.md        # START HERE — architecture, field formats, new-platform recipe
│   ├── actionbuilder_tags.md   # Verbatim AB OSDI API reference: tags
│   ├── actionbuilder_person_signup.md  # Verbatim AB OSDI API reference: Person Signup Helper
│   ├── assessment_rules.md     # Auto-assessment level criteria and write policy
│   ├── deduplication.md        # Dedup strategy, execution log, edge cases
│   ├── organizing_team_build_plan.md   # Campaign-26 build plan (executed 2026-06; historical)
│   └── update_records_incident_2026-03-21.md  # Rate limit incident postmortem
│
├── dbt_project.yml             # dbt project config
├── profiles.yml                # BigQuery connection config (reads keyfile from env var)
├── dbt.sh                      # Shell entry point: just calls run_dbt.py
├── run_dbt.py                  # Credential loader + dbt subprocess wrapper
├── _query_test_campaign.py     # Ad-hoc diagnostic: campaigns/fields in updates_needed
└── _spot_check_views.py        # Ad-hoc diagnostic: spot-check test-campaign views
```

---

## Data Flow

```
External Platforms          Staging Models             Core Models               Sync Job
──────────────────          ──────────────             ───────────               ────────
Mobilize               ──► mobilize_event_data    ──┐
                       ──► ofp_attendance            │
Action Network         ──► action_network_6mo_actions┤
                       ──► state/natl_top_performers ├──► correct_participation_values ──┐
ScaleToWin             ──► scaletowin_call_data      │                                   │
NewMode                ──► newmode_actions           │                                   ├──► updates_needed ──► sync script
Soapboxx               ──► soapboxx_stories       ──┘                                   │
                                                                                         │
ActionBuilder DB ──► (actionbuilder_cleaned.*) ──► current_tag_values ────────────────┘
```

`updates_needed` reads from both sides and outputs only the rows where the correct value differs from the current value, formatted for the sync script.

---

## BigQuery Datasets

| Dataset | Role |
|---------|------|
| `actionbuilder_sync` | **This project's output** — all views managed here |
| `actionbuilder_cleaned` | Cleaned AB database tables (`cln_actionbuilder__*`) |
| `core_enhanced` | Cross-platform identity hub (links emails/phones to person_ids) |
| `mobilize_cleaned` | Cleaned Mobilize event participation data |
| `actionnetwork_cleaned` | Cleaned Action Network user/action data |
| `scaletowin_dialer_cleaned` | Cleaned ScaleToWin call records |
| `ep_archive` | EP internal shift data |
| `actionnetwork_views` | Reference tables (states, etc.) |
| `targetsmart_enhanced` | Voter file (address fallback in new-record load) |

---

## Key Files Outside This Repo

- **ccef-connections library** — at `../AI Interpretation/ccef-connections` ([GitHub](https://github.com/common-cause/ccef_connections)); provides `BigQueryConnector`, `ActionBuilderConnector`, and other service connectors. All Python scripts in this project use it for BQ and AB API access.
- **Credentials** — stored in `.env` (gitignored). `BIGQUERY_CREDENTIALS_PASSWORD` (BQ service account JSON) and `ACTION_BUILDER_CREDENTIALS_PASSWORD` (AB API token JSON).

---

## BigQuery MCP (Claude Code)

When working in Claude Code, the global `bigquery` MCP is available and pre-approved for this project. Use it to query views directly without leaving the conversation:

```
bq_query("SELECT * FROM actionbuilder_sync.updates_needed LIMIT 10")
bq_list_tables("actionbuilder_sync")
```

The MCP connects to `proj-tmc-mem-com` using the shared `BIGQUERY_CREDENTIALS_PASSWORD` service account from the meta-project `.env`. No per-project credential setup is required for Claude Code queries — only `dbt` / `run_dbt.py` reads the local `.env`.

---

## Detailed Documentation

- **[docs/sync_overview.md](docs/sync_overview.md)** — **Start here.** Sync architecture, field list, tag removal, Organizing Team campaign, the recipe for incorporating a new platform
- **[civis/SCHEDULED_SCRIPTS.md](civis/SCHEDULED_SCRIPTS.md)** — Authoritative inventory of Civis jobs, the nightly workflow steps, and job IDs
- **[docs/actionbuilder_tags.md](docs/actionbuilder_tags.md)** / **[docs/actionbuilder_person_signup.md](docs/actionbuilder_person_signup.md)** — Verbatim ActionBuilder OSDI API references
- **[docs/assessment_rules.md](docs/assessment_rules.md)** — Auto-assessment level criteria and write policy
- **[docs/deduplication.md](docs/deduplication.md)** — Deduplication strategy, root cause, deletion workflow
- **[docs/organizing_team_build_plan.md](docs/organizing_team_build_plan.md)** — Campaign-26 build plan (executed June 2026; kept for the verified API semantics)
- **[docs/update_records_incident_2026-03-21.md](docs/update_records_incident_2026-03-21.md)** — Rate limit incident postmortem

---

## Roadmap

1. **[Done]** Manage views as code via dbt (replaced BQ GUI)
2. **[Done]** Tag removal — sync replaces values rather than accumulating them
3. **[Done]** Dedup execution — 8,921 entities removed, 154 emails + 91 phones migrated, 3,532 new entities inserted
4. **[Done]** Sync log architecture — `sync_log` BQ table + dbt wrapper views compensate for BQ replication gaps
5. **[Done]** Auto-assessments — `auto_assessment_rules` + `apply_assessments` operation, upgrade-only write policy
6. **[Done]** OFP training tags — now the **universal** `Trainings > Organizing For Power` field (one network-level tag); attendance synced via Mobilize timeslot mapping and is a load qualifier
7. **[Done]** `taggable_logbook` replication — AB fixed their internal mirror (~2026-03-21); overlay model retained for hard-delete gap
8. **[Done]** Nightly maintenance — Civis workflow #119217 runs run_dbt → insert_new_records → update_records → apply_assessments → append_notes → connect_entities → insert_organizing_team → assign_organizers at 10 PM ET
9. **[Done]** Organizing Team campaign (id 26) — OFP attendees connected/inserted into the crosscutting campaign with the universal OFP field
10. **[Done]** Soapboxx storytelling — tag + load qualifier + hot-prospect signal (2026-06-11)
11. **[Done]** Mobilize anti-poaching — Rules A/B keep other groups' EP volunteers out of the load path (2026-06-17)
12. **[Done]** Organizer assignment — campaign-26 members wired to their assigned organizer via People:People connections (2026-07-03; Lamair's 141 assigned 2026-07-29). Unstaffed states + territories route to Tiffany Rubio (organizing intern): her 71 assigned 2026-07-29 after the `remove_ot_duplicates` cleanup removed 70 double-insert twins from campaign 26 (bug fixed with a sync_log overlay). All five organizers live; all 87 campaign-26 duplicate pairs resolved 2026-07-29 (70 bug twins + 17 triaged)
13. **[Active]** 1MC rollout — notes append nightly; tag columns staged in `updates_needed`, not yet wired into `TAG_COLS`
14. **[Active]** Resolve open `dedup_unresolved` pairs (16 same-campaign ambiguous pairs)
15. **[Planned]** Slack alerting / replication sentinel — waiting on IT for Slack app + webhook
16. **[Future]** New data flows: Airtable, Zoom, Mobilize relational organizing campaign
