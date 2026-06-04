#!/usr/bin/env bash
# Civis entrypoint — remove EP externals (one-shot cleanup).
# GitHub-backed job: Civis clones this repo into app/, so set the job body to:
#     bash app/civis/remove_ep_externals.sh
# Edit this file (not the Civis UI) to change setup/run steps.
#
# One-shot cleanup: remove partner-org EP volunteers loaded into AB only via
# the old EP-as-decisive-qualifier path (now disallowed by the source_code
# filter and EP-org Mobilize exclusion in master_load_qualifiers).
#
# Reads from actionbuilder_sync.ep_external_removal.
# Logs each delete to actionbuilder_sync.sync_log with operation='remove_ep_external'.
#
# Run with --dry-run first to preview the deletes; then re-run without the flag
# to execute. After execution, archive or remove this file — it is not part of
# the recurring nightly workflow.

# Pinned to a ccef-connections release tag — bump deliberately when upgrading.
pip install "ccef-connections[bigquery] @ git+https://github.com/common-cause/ccef_connections.git@v0.2.0"

DELAY="--delay 0.3"

# Dry-run first: preview the entities that would be deleted.
# Comment out the dry-run line and uncomment the live line below to execute.
python app/scripts/sync.py remove_ep_externals --dry-run $DELAY

# Live execution (uncomment when ready):
# python app/scripts/sync.py remove_ep_externals $DELAY
