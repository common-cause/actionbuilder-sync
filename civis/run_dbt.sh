#!/usr/bin/env bash
# Civis entrypoint — dbt run (intended nightly workflow STEP 0, before AB Inserts).
# GitHub-backed job: Civis clones this repo into app/, so set the job body to:
#     bash app/civis/run_dbt.sh
# Edit this file (not the Civis UI) to change setup/run steps.
#
# Purpose: refresh all actionbuilder_sync models so the sync ops that follow read current
# data. Views are recreated cheaply; table-materialized models (e.g. master_load_qualifiers,
# once flipped) are recomputed here. This is why it MUST run FIRST: a table only reflects new
# Mobilize/AN/etc. data after a dbt run, so every downstream step depends on it having run.
# If dbt fails, this exits non-zero and the workflow stops — better than syncing stale data.

set -eo pipefail

# Pin dbt to the version used locally (dbt-core 1.11.x). Bump deliberately when upgrading,
# mirroring the ccef-connections pin in the other civis scripts.
pip install "dbt-bigquery==1.11.0" "python-dotenv>=1.0"

# run_dbt.py loads BIGQUERY_CREDENTIALS_PASSWORD (a Civis job env var here; .env locally),
# writes it to a temp keyfile, and runs dbt with the project's profiles.yml. Builds every model.
python app/run_dbt.py run
