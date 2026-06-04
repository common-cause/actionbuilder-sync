#!/usr/bin/env bash
# Civis entrypoint — AB Assessment Setting (nightly workflow step 3).
# GitHub-backed job: Civis clones this repo into app/, so set the job body to:
#     bash app/civis/apply_assessments.sh
# Edit this file (not the Civis UI) to change setup/run steps.

pip install "ccef-connections[bigquery] @ git+https://github.com/common-cause/ccef_connections.git"

python app/scripts/sync.py apply_assessments --delay 0.3
