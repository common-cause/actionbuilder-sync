This project manages the `actionbuilder_sync` BigQuery dataset via dbt. Views feed participation data from Mobilize, Action Network, and ScaleToWin into ActionBuilder (Common Cause's organizing CRM). The sync script (provided by The Movement Cooperative) reads `updates_needed` and makes ActionBuilder API calls.

## BigQuery MCP

The global `bigquery` MCP is active and pre-approved for this project. Use `bq_query(sql)` and `bq_list_tables(dataset)` to inspect views, spot-check data, or debug sync issues without leaving the conversation. Connects to `proj-tmc-mem-com` using the shared service account.

Example:
```
bq_query("SELECT * FROM actionbuilder_sync.updates_needed LIMIT 5")
bq_list_tables("actionbuilder_sync")
```

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

## Current State

- Tag updates: active and running
- Deduplication: views built; waiting on TMC consultant for operation column formats before executing deletion
- New record insertion: built and fully guarded; ready to activate after dedup

## Key Datasets

| Dataset | Role |
|---------|------|
| `actionbuilder_sync` | This project's output (all views) |
| `actionbuilder_cleaned` | Cleaned AB database tables |
| `core_enhanced` | Cross-platform identity hub |
| `mobilize_cleaned` | Mobilize event participation |
| `actionnetwork_cleaned` | Action Network users/actions |
| `scaletowin_dialer_cleaned` | ScaleToWin calls |
