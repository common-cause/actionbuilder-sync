# ActionBuilder Sync

Keeps participation data in [ActionBuilder](https://actionbuilder.org) current by syncing activity from Common Cause's other platforms. Built as a dbt project running entirely in BigQuery.

---

## What It Does

Reads participation records from Mobilize, Action Network, and ScaleToWin, computes the correct tag values for each activist in ActionBuilder, compares them against what ActionBuilder currently shows, and outputs a table of changes. A custom sync script (provided by The Movement Cooperative) reads that table and makes the ActionBuilder API calls.

**Current state:**
- Updating tag values on existing records — active and running
- New record insertion — built but not yet active; pending deduplication cleanup
- Deduplication — `dedup_candidates` view is built and ready; deletions via API not yet executed

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
BIGQUERY_API_CREDENTIALS_PASSWORD={"type":"service_account","project_id":"proj-tmc-mem-com",...}
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
# Deploy all 13 views to BigQuery
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
│   ├── dedup_candidates.sql               # Entities to delete from AB (run before new inserts)
│   │
│   │   ── New record insertion (not yet active) ──
│   ├── master_load_qualifiers.sql         # People who qualify for AB entry
│   ├── deduplicated_names_to_load.sql     # Final new-record insertion feed
│   │
│   │   ── Diagnostics ──
│   ├── identity_resolution.sql            # Entity → person_id mapping with data-source flags
│   └── entity_lookup_debug.sql            # Entity names, emails, phones for spot-checking
│
├── docs/
│   └── sync_overview.md        # Detailed sync architecture, field formats, known issues
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

- **The sync script** — provided by The Movement Cooperative consultant; reads `actionbuilder_sync.updates_needed` and makes AB API calls. Not version-controlled here.
- **Credentials** — stored in `.env` (gitignored). The `BIGQUERY_API_CREDENTIALS_PASSWORD` env var holds the full service account JSON.
- **ccef-connections library** — at `../AI Interpretation/ccef-connections`; provides BigQuery, Airtable, Zoom, Sheets, Action Network connectors. Used for any Python scripts that need to query BigQuery directly.

---

## Detailed Documentation

- **[docs/sync_overview.md](docs/sync_overview.md)** — ActionBuilder sync format, field list, tag removal, known issues, how to add new fields
- **[docs/deduplication.md](docs/deduplication.md)** — Deduplication strategy, duplicate counts, root cause analysis, deletion workflow

---

## Roadmap

1. **[Done]** Manage views as code via dbt (replaced BQ GUI)
2. **[Done]** Tag removal — sync now replaces values rather than accumulating them
3. **[Done]** `dedup_candidates` view — identifies 374 entities to delete
4. **[Next]** Execute deduplication — delete candidates via AB API, verify sync recovers
5. **[Next]** Enable new record insertion via `deduplicated_names_to_load`
6. **[Future]** New data flows: Airtable, Zoom, Mobilize relational organizing campaign
