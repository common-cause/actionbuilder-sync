#!/usr/bin/env bash
# Civis entrypoint — AB Assessment Setting (nightly workflow step 3).
# GitHub-backed job: Civis clones this repo into app/, so set the job body to:
#     bash app/civis/apply_assessments.sh
# Edit this file (not the Civis UI) to change setup/run steps.

# Pinned to a ccef-connections release tag — bump deliberately when upgrading.
pip install "ccef-connections[bigquery] @ git+https://github.com/common-cause/ccef_connections.git@v0.2.0"

python app/scripts/sync.py apply_assessments --delay 0.3
