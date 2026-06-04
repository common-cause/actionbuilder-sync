#!/usr/bin/env bash
# Civis entrypoint — AB Tag Updates (nightly workflow step 2).
# GitHub-backed job: Civis clones this repo into app/, so set the job body to:
#     bash app/civis/update_records.sh
# Edit this file (not the Civis UI) to change setup/run steps.

pip install "ccef-connections[bigquery] @ git+https://github.com/common-cause/ccef_connections.git"

python app/scripts/sync.py update_records --campaign arizona --delay 0.3
python app/scripts/sync.py update_records --campaign california --delay 0.3
python app/scripts/sync.py update_records --campaign colorado --delay 0.3
python app/scripts/sync.py update_records --campaign florida --delay 0.3
python app/scripts/sync.py update_records --campaign georgia --delay 0.3
python app/scripts/sync.py update_records --campaign hawaii --delay 0.3
python app/scripts/sync.py update_records --campaign illinois --delay 0.3
python app/scripts/sync.py update_records --campaign indiana --delay 0.3
python app/scripts/sync.py update_records --campaign maryland --delay 0.3
python app/scripts/sync.py update_records --campaign massachusetts --delay 0.3
python app/scripts/sync.py update_records --campaign michigan --delay 0.3
python app/scripts/sync.py update_records --campaign minnesota --delay 0.3
python app/scripts/sync.py update_records --campaign nebraska --delay 0.3
python app/scripts/sync.py update_records --campaign new_mexico --delay 0.3
python app/scripts/sync.py update_records --campaign new_york --delay 0.3
python app/scripts/sync.py update_records --campaign north_carolina --delay 0.3
python app/scripts/sync.py update_records --campaign ohio --delay 0.3
python app/scripts/sync.py update_records --campaign oregon --delay 0.3
python app/scripts/sync.py update_records --campaign pennsylvania --delay 0.3
python app/scripts/sync.py update_records --campaign rhode_island --delay 0.3
python app/scripts/sync.py update_records --campaign texas --delay 0.3
python app/scripts/sync.py update_records --campaign virginia --delay 0.3
python app/scripts/sync.py update_records --campaign wisconsin --delay 0.3
python app/scripts/sync.py update_records --campaign dc --delay 0.3
