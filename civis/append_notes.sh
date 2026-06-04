#!/usr/bin/env bash
# Civis entrypoint — AB Notes Append (nightly workflow step 4).
# GitHub-backed job: Civis clones this repo into app/, so set the job body to:
#     bash app/civis/append_notes.sh
# Edit this file (not the Civis UI) to change setup/run steps.

pip install "ccef-connections[bigquery] @ git+https://github.com/common-cause/ccef_connections.git"

python app/scripts/sync.py append_notes --delay 0.3
