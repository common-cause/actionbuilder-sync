This project manages the `actionbuilder_sync` BigQuery dataset via dbt. Views feed participation data from Mobilize, Action Network, and ScaleToWin into ActionBuilder (Common Cause's organizing CRM). The sync script (provided by The Movement Cooperative) reads `updates_needed` and makes ActionBuilder API calls.

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

## Current State (as of 2026-03-12)

- Tag updates: active and running
- Deduplication: EXECUTED — 154 emails migrated, 91 phones migrated, 8,921 entities removed from campaigns
- New record insertion: EXECUTED — 3,532 entities inserted 2026-03-12 (expect in BQ 2026-03-13)
- Sync log: BUILT AND DEPLOYED — `actionbuilder_sync.sync_log` table live; sync.py instruments remove_records and insert_new_records

## BQ Replication Gaps (known, reported to TMC 2026-03-12)

1. **Hard deletes never replicated** — `campaigns_entities` removals have no `updated_at` change;
   BQ perpetually shows removed entities. Mitigated by sync_log (dedup_candidates wrapper).
2. **`taggable_logbook` stale since 2026-03-05** — table too large for TMC's 30s query window.
   `current_tag_values` and `updates_needed` are working from stale tag data until this is fixed.
   Resolution path TBD — may require workaround similar to sync_log.

## Sync Log Architecture

Two dbt models exist in paired versions:
- `dedup_candidates_bq_only` — original BQ-only logic (used by dedup_ambiguous)
- `dedup_candidates` — thin wrapper filtering out entities already logged as removed
- `deduplicated_names_to_load_bq_only` — original BQ-only logic
- `deduplicated_names_to_load` — thin wrapper filtering out person_ids already logged as inserted

To revert to BQ-only mode: delete the two wrapper models, rename `_bq_only` files back,
revert dedup_ambiguous ref. The sync_log table stays as a permanent audit log.

## Key Datasets

| Dataset | Role |
|---------|------|
| `actionbuilder_sync` | This project's output (all views) |
| `actionbuilder_cleaned` | Cleaned AB database tables |
| `core_enhanced` | Cross-platform identity hub |
| `mobilize_cleaned` | Mobilize event participation |
| `actionnetwork_cleaned` | Action Network users/actions |
| `scaletowin_dialer_cleaned` | ScaleToWin calls |
