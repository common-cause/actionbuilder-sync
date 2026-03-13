# ActionBuilder Sync

Keeps participation data in [ActionBuilder](https://actionbuilder.org) current by syncing activity from Common Cause's other platforms. Built as a dbt project running entirely in BigQuery.

---

## What It Does

Reads participation records from Mobilize, Action Network, and ScaleToWin, computes the correct tag values for each activist in ActionBuilder, compares them against what ActionBuilder currently shows, and outputs a table of changes. A custom sync script (provided by The Movement Cooperative) reads that table and makes the ActionBuilder API calls.

**Current state (2026-03-12):**
- Tag updates — active and running; `taggable_logbook` replication stale (TMC issue, workaround TBD)
- Deduplication — executed: 154 emails migrated, 91 phones migrated, 8,921 entities removed from campaigns
- New record insertion — executed: 3,532 entities inserted 2026-03-12
- Sync log — live: `actionbuilder_sync.sync_log` records API calls so dbt views stay correct despite BQ replication gaps

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
# Deploy all 25 views to BigQuery
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
│   ├── state_action_network_top_performers.sql
│   │
│   │   ── Core sync pipeline ──
│   ├── correct_participation_values.sql   # "What should be in AB" (computed values)
│   ├── current_tag_values.sql             # "What is currently in AB" (from taggable_logbook)
│   ├── updates_needed.sql                 # Sync job input — rows to change
│   │
│   │   ── Deduplication ──
│   ├── dedup_candidates.sql               # Sync-log filtered wrapper (used by sync.py)
│   ├── dedup_candidates_bq_only.sql       # BQ-only version (used by dedup_ambiguous)
│   ├── dedup_ambiguous.sql                # Ambiguous pairs for human/AI review
│   ├── dedup_unresolved.sql               # dedup_ambiguous minus resolved pairs
│   ├── email_migration_needed.sql         # Emails to copy to keeper entities before deletion
│   ├── phone_migration_needed.sql         # Phones to copy to keeper entities before deletion
│   │
│   │   ── New record insertion ──
│   ├── master_load_qualifiers.sql         # People who qualify for AB entry
│   ├── deduplicated_names_to_load.sql     # Sync-log filtered wrapper (used by sync.py)
│   └── deduplicated_names_to_load_bq_only.sql  # BQ-only version
│   │
│   │   ── Diagnostics ──
│   ├── identity_resolution.sql            # Entity → person_id mapping with data-source flags
│   └── entity_lookup_debug.sql            # Entity names, emails, phones for spot-checking
│
├── civis/                      # Shell scripts for Civis Platform job deployment
│   ├── cleanup_duplicate_tags.sh
│   ├── remove_duplicate_entities.sh
│   └── insert_new_records.sh
│
├── scripts/                    # Python sync scripts
│   ├── sync.py                 # Main sync script (all operations)
│   └── create_sync_log.sql     # One-time DDL to create sync_log table (already run)
│
├── docs/
│   ├── sync_overview.md        # Detailed sync architecture, field formats, known issues
│   └── deduplication.md        # Dedup strategy, execution log, edge cases
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

- **The sync script** — `scripts/sync.py` in this repo; reads dbt views and makes AB API calls. Operations: `update_records`, `insert_new_records`, `remove_records`, `prepare_email_data`, `prepare_phone_data`, `apply_assessments`.
- **Credentials** — stored in `.env` (gitignored). The `BIGQUERY_CREDENTIALS_PASSWORD` env var holds the full service account JSON.
- **ccef-connections library** — at `../AI Interpretation/ccef-connections`; provides BigQuery, Airtable, Zoom, Sheets, Action Network connectors. Used for any Python scripts that need to query BigQuery directly.

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

- **[docs/sync_overview.md](docs/sync_overview.md)** — ActionBuilder sync format, field list, tag removal, known issues, how to add new fields
- **[docs/deduplication.md](docs/deduplication.md)** — Deduplication strategy, duplicate counts, root cause analysis, deletion workflow

---

## Roadmap

1. **[Done]** Manage views as code via dbt (replaced BQ GUI)
2. **[Done]** Tag removal — sync now replaces values rather than accumulating them
3. **[Done]** `dedup_candidates` — identifies duplicate AB entities across five tiers
4. **[Done]** `email_migration_needed` / `phone_migration_needed` — contact info consolidated before deletion (154 emails, 91 phones migrated)
5. **[Done]** `deduplicated_names_to_load` — new-record insertion feed, fully guarded against re-creating existing entities
6. **[Done]** Dedup execution — 8,921 entities removed from campaigns, 3,532 new entities inserted (2026-03-12)
7. **[Done]** Sync log architecture — `sync_log` BQ table + dbt wrapper views compensate for BQ replication gaps; sync.py instruments remove_records and insert_new_records
8. **[Active]** Resolve open `dedup_unresolved` pairs (currently 16 same-campaign ambiguous pairs)
9. **[Active / TMC-blocked]** `taggable_logbook` replication fix — table too large for TMC's query window; `current_tag_values` and `updates_needed` work from stale data until resolved. Workaround path TBD.
10. **[Future]** New data flows: Airtable, Zoom, Mobilize relational organizing campaign
