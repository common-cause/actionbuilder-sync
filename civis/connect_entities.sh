#!/usr/bin/env bash
# Civis entrypoint — AB Organizing Team Connect (nightly workflow step 5).
# GitHub-backed job: Civis clones this repo into app/, so set the job body to:
#     bash app/civis/connect_entities.sh
# Edit this file (not the Civis UI) to change setup/run steps.

# Pinned to a ccef-connections release tag — bump deliberately when upgrading.
pip install "ccef-connections[bigquery] @ git+https://github.com/common-cause/ccef_connections.git@v0.2.0"

python app/scripts/sync.py connect_entities --delay 0.3
