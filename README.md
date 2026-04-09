# ActionBuilder Sync

Keeps participation data in [ActionBuilder](https://actionbuilder.org) current by syncing activity from Common Cause's other platforms. Built as a dbt project running entirely in BigQuery.

---

## What It Does

Reads participation records from Mobilize, Action Network, and ScaleToWin, computes the correct tag values for each activist in ActionBuilder, compares them against what ActionBuilder currently shows, and outputs a table of changes. A custom sync script (provided by The Movement Cooperative) reads that table and makes the ActionBuilder API calls.

**Current state (2026-03-30):**
- Nightly maintenance — running on Civis at 10 PM ET: `insert_new_records` → `update_records` → `apply_assessments`. Operational since 2026-03-25.
- Tag updates — `update_records` running nightly across all 24 state campaigns
- Assessments — `apply_assessments` sets engagement levels automatically (upgrade-only); see `docs/assessment_rules.md`
- Deduplication — executed: 154 emails migrated, 91 phones migrated, 8,921 entities removed from campaigns
- New record insertion — nightly; 3,532 initial entities inserted 2026-03-12, ongoing inserts since
- Sync log — live: `actionbuilder_sync.sync_log` records all API operations with per-tag granularity
- AB mirror bug — `taggable_logbook` replication restored ~2026-03-21 (was stalled 3/5–3/20); overlay model remains for hard-delete gap coverage

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
# Deploy all 27 models to BigQuery
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
├── models/                     # All dbt models (deployed as BigQuery views)
│   ├── schema.yml              # Model descriptions and data tests
│   │
│   │   ── Source/staging ──
│   ├── action_network_actions.sql         # Raw AN actions joined to users
│   ├── action_network_6mo_actions.sql     # AN actions filtered to 6 months
│   ├── mobilize_event_data.sql            # Mobilize attendance aggregated by email
│   ├── scaletowin_call_data.sql           # ScaleToWin calls aggregated by phone
│   ├── newmode_actions.sql                # NewMode letter submissions
│   ├── ofp_attendance.sql                 # Organizing for Power training attendance
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
│   │   ── Deduplication ──
│   ├── dedup_candidates.sql               # Sync-log filtered wrapper (used by sync.py)
│   ├── dedup_candidates_bq_only.sql       # BQ-only version (used by dedup_ambiguous)
│   ├── dedup_ambiguous.sql                # Ambiguous pairs for human/AI review (TABLE)
│   ├── dedup_unresolved.sql               # dedup_ambiguous minus resolved pairs
│   ├── email_migration_needed.sql         # Emails to copy to keeper entities before deletion
│   ├── phone_migration_needed.sql         # Phones to copy to keeper entities before deletion
│   │
│   │   ── New record insertion ──
│   ├── master_load_qualifiers.sql         # People who qualify for AB entry
│   ├── deduplicated_names_to_load.sql     # Sync-log filtered wrapper (used by sync.py)
│   ├── deduplicated_names_to_load_bq_only.sql  # BQ-only version
│   │
│   │   ── Diagnostics ──
│   ├── identity_resolution.sql            # Entity → person_id mapping with data-source flags
│   ├── entity_lookup_debug.sql            # Entity names, emails, phones for spot-checking
│   ├── test_campaign_updates.sql          # Test campaign pending changes (human-readable)
│   └── test_campaign_update_summary.sql   # Test campaign change summary dashboard
│
├── civis/                      # Shell scripts for Civis Platform job deployment
│   ├── insert_new_records.sh        # Nightly: add new entities
│   ├── update_records.sh            # Nightly: sync tag values
│   ├── apply_assessments.sh         # Nightly: set assessment levels
│   ├── snapshot_tag_state.sh        # On-demand: capture tag ground truth from API
│   ├── cleanup_duplicate_tags.sh    # On-demand: remove duplicate taggings
│   └── remove_duplicate_entities.sh # One-time: dedup execution
│
├── scripts/                    # Python sync scripts
│   ├── sync.py                 # Main sync script (all operations)
│   ├── cleanup_duplicate_tags.py  # Standalone: delete duplicate taggings
│   ├── capture_ab_evidence.py  # One-time: AB bug report (mirror staleness)
│   ├── targeted_evidence.py    # One-time: deletion-check and write-check evidence
│   ├── check_bq_refresh.py     # Utility: check BQ table freshness
│   ├── check_recent_inserts.py # Utility: verify recent entity insertions
│   ├── add_tags_to_campaigns.py   # One-time: add tag fields to campaigns
│   ├── add_ofp_field_to_campaigns.py  # One-time: add OFP training field
│   └── create_sync_log.sql     # One-time DDL to create sync_log table (already run)
│
├── evidence/                   # Output from evidence scripts (gitignored JSON/TXT reports)
│
├── seeds/                      # dbt seed CSVs
│   ├── state_an_thresholds.csv # Per-state AN action thresholds (MI=5, NE=5, default=20)
│   └── ofp_training_map.csv    # Mobilize timeslot → OFP training name mapping
│
├── docs/
│   ├── sync_overview.md        # Sync architecture, field formats, known issues
│   ├── deduplication.md        # Dedup strategy, execution log, edge cases
│   ├── assessment_rules.md     # Auto-assessment level criteria and write policy
│   └── update_records_incident_2026-03-21.md  # Rate limit incident postmortem
│
├── dbt_project.yml             # dbt project config
├── profiles.yml                # BigQuery connection config (reads keyfile from env var)
├── dbt.sh                      # Shell entry point: just calls run_dbt.py
└── run_dbt.py                  # Credential loader + dbt subprocess wrapper
```

---

## Data Flow

```
External Platforms          Staging Models             Core Models               Sync Job
──────────────────          ──────────────             ───────────               ────────
Mobilize               ──► mobilize_event_data    ──┐
Action Network         ──► action_network_6mo_actions ┤
                       ──► state_an_top_performers    ├──► correct_participation_values ──┐
ScaleToWin             ──► scaletowin_call_data    ──┘                                    ├──► updates_needed ──► sync script
                                                                                           │
ActionBuilder DB ──► (actionbuilder_cleaned.*) ──► current_tag_values ─────────────────┘
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

- **[docs/sync_overview.md](docs/sync_overview.md)** — Sync architecture, field list, tag removal, known issues, how to add new fields
- **[docs/deduplication.md](docs/deduplication.md)** — Deduplication strategy, root cause, deletion workflow
- **[docs/assessment_rules.md](docs/assessment_rules.md)** — Auto-assessment level criteria and write policy
- **[docs/update_records_incident_2026-03-21.md](docs/update_records_incident_2026-03-21.md)** — Rate limit incident postmortem

---

## Roadmap

1. **[Done]** Manage views as code via dbt (replaced BQ GUI)
2. **[Done]** Tag removal — sync replaces values rather than accumulating them
3. **[Done]** Dedup execution — 8,921 entities removed, 154 emails + 91 phones migrated, 3,532 new entities inserted
4. **[Done]** Sync log architecture — `sync_log` BQ table + dbt wrapper views compensate for BQ replication gaps
5. **[Done]** Auto-assessments — `auto_assessment_rules` + `apply_assessments` operation, upgrade-only write policy
6. **[Done]** OFP training tags — Organizing for Power attendance synced via Mobilize timeslot mapping
7. **[Done]** `taggable_logbook` replication — AB fixed their internal mirror (~2026-03-21); overlay model retained for hard-delete gap
8. **[Done]** Nightly maintenance — Civis runs insert_new_records → update_records → apply_assessments at 10 PM ET
9. **[Active]** Resolve open `dedup_unresolved` pairs (16 same-campaign ambiguous pairs)
10. **[Planned]** Slack alerting / replication sentinel — waiting on IT for Slack app + webhook
11. **[Future]** New data flows: Airtable, Zoom, Mobilize relational organizing campaign
